package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"golang.org/x/exp/rand"
)

// this func is only invoked by main node
func (p *PbftConsensusNode) Propose() {
	// wait other nodes to start TCPlistening, sleep 5 sec.
	time.Sleep(5 * time.Second)

	nextRoundBeginSignal := make(chan bool)

	go func() {
		// go into the next round
		for {
			time.Sleep(time.Duration(int64(p.pbftChainConfig.BlockInterval)) * time.Millisecond)
			// send a signal to another GO-Routine. It will block until a GO-Routine try to fetch data from this channel.
			for p.pbftStage.Load() != 1 {
				time.Sleep(time.Millisecond * 100)
			}
			nextRoundBeginSignal <- true
		}
	}()

	go func() {
		// check whether to view change
		for {
			time.Sleep(time.Second)
			if time.Now().UnixMilli()-p.lastCommitTime.Load() > int64(params.PbftViewChangeTimeOut) {
				p.lastCommitTime.Store(time.Now().UnixMilli())
				go p.viewChangePropose()
			}
		}
	}()

	for {
		select {
		case <-nextRoundBeginSignal:
			go func() {
				// if this node is not leader, do not propose.
				if uint64(p.view.Load()) != p.NodeID {
					return
				}

				p.sequenceLock.Lock()
				p.pl.Plog.Printf("S%dN%d get sequenceLock locked, now trying to propose...\n", p.ShardID, p.NodeID)
				// propose
				// implement interface to generate propose
				_, r, txNumInBlock := p.ihm.HandleinPropose()
				p.txNumInBlock = txNumInBlock

				digest := getDigest(r)
				p.requestPool[string(digest)] = r
				p.pl.Plog.Printf("S%dN%d put the request into the pool ...\n", p.ShardID, p.NodeID)

				ppmsg := message.PrePrepare{
					RequestMsg: r,
					Digest:     digest,
					SeqID:      p.sequenceID,
				}

				//根据上个区块哈希和prepare、commit情况进行更新节点行为记录更新
				if !p.lastBlockIfPartion && !p.lastBlockIfSuccess && p.lastDigest != "" {
					p.RecordFailBlockNodeAction()
				}
				p.lastDigest = string(digest)

				p.lastBlockIfSuccess = false
				if r.RequestType == message.PartitionReq {
					p.lastBlockIfPartion = true
				} else {
					p.lastBlockIfPartion = false
				}
				p.height2Digest[p.sequenceID] = string(digest)
				// marshal and broadcast
				ppbyte, err := json.Marshal(ppmsg)
				if err != nil {
					log.Panic()
				}
				msg_send := message.MergeMessage(message.CPrePrepare, ppbyte)
				networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
				networks.TcpDial(msg_send, p.RunningNode.IPaddr)
				p.pbftStage.Store(2)
			}()

		case <-p.pStop:
			p.pl.Plog.Printf("S%dN%d get stopSignal in Propose Routine, now stop...\n", p.ShardID, p.NodeID)
			return
		}
	}
}

// 区块提交失败了，记录节点贡献交易、投票结果的函数
func (p *PbftConsensusNode) RecordFailBlockNodeAction() {
	nodeIfPrepare := p.cntPrepareConfirm[p.lastDigest]
	nodeIfCommit := p.cntCommitConfirm[p.lastDigest]
	nodeRecorded := make(map[uint64]bool)
	//失败就默认主节点错误行为
	if _, exist := p.nodeFR[uint64(p.view.Load())]; !exist {
		p.nodeMW[uint64(p.view.Load())] = 1
	} else {
		p.nodeMW[uint64(p.view.Load())]++
	}
	nodeRecorded[uint64(p.view.Load())] = true

	for Node, ifCommit := range nodeIfCommit {
		if ifCommit { //认为对提交失败的区块投commit是错误行为
			if Node.NodeID != uint64(p.view.Load()) {
				if _, exist := p.nodeFR[Node.NodeID]; !exist {
					p.nodeFW[Node.NodeID] = 1
				} else {
					p.nodeFW[Node.NodeID]++
				}
			}
			nodeRecorded[Node.NodeID] = true
		}
	}
	for Node, ifPrepare := range nodeIfPrepare {
		if ifPrepare && !nodeRecorded[Node.NodeID] { //认为对提交失败的区块投prepare是错误行为
			if Node.NodeID != uint64(p.view.Load()) {
				if _, exist := p.nodeFR[Node.NodeID]; !exist {
					p.nodeFW[Node.NodeID] = 1
				} else {
					p.nodeFW[Node.NodeID]++
				}
			}
			nodeRecorded[Node.NodeID] = true
		}
	}
	//贡献交易计算
	nodeWrongNum := uint64(len(nodeRecorded)) //有记录证明在区块提交失败时进行了错误行为
	nodeRightNum := p.node_nums - nodeWrongNum
	for i := uint64(0); i < p.node_nums; i++ {
		if nodeRecorded[i] {
			p.TXi[i] = p.TXi[i] - float32(p.txNumInBlock)/float32(nodeWrongNum)
		} else {
			if i == uint64(p.view.Load()) {
				if _, exist := p.nodeFR[i]; !exist {
					p.nodeMR[i] = 1
				} else {
					p.nodeMR[i]++
				}
			} else {
				if _, exist := p.nodeFR[i]; !exist {
					p.nodeFR[i] = 1
				} else {
					p.nodeFR[i]++
				}
			}
			p.TXi[i] = p.TXi[i] + float32(p.txNumInBlock)/float32(nodeRightNum)
		}
	}
}

// Handle pre-prepare messages here.
// If you want to do more operations in the pre-prepare stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinPrePrepare**
func (p *PbftConsensusNode) handlePrePrepare(content []byte) {
	p.RunningNode.PrintNode()
	fmt.Println("received the PrePrepare ...")
	// decode the message
	ppmsg := new(message.PrePrepare)
	err := json.Unmarshal(content, ppmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	for p.pbftStage.Load() < 1 && ppmsg.SeqID >= p.sequenceID && p.view.Load() == curView {
		p.conditionalVarpbftLock.Wait()
	}
	defer p.conditionalVarpbftLock.Broadcast()

	// if this message is out of date, return.
	if ppmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	flag := false
	if digest := getDigest(ppmsg.RequestMsg); string(digest) != string(ppmsg.Digest) {
		p.pl.Plog.Printf("S%dN%d : the digest is not consistent, so refuse to prepare. \n", p.ShardID, p.NodeID)
	} else if p.sequenceID < ppmsg.SeqID {
		p.requestPool[string(getDigest(ppmsg.RequestMsg))] = ppmsg.RequestMsg
		p.height2Digest[ppmsg.SeqID] = string(getDigest(ppmsg.RequestMsg))
		p.pl.Plog.Printf("S%dN%d : the Sequence id is not consistent, so refuse to prepare. \n", p.ShardID, p.NodeID)
	} else {
		// do your operation in this interface
		flag = p.ihm.HandleinPrePrepare(ppmsg)
		p.requestPool[string(getDigest(ppmsg.RequestMsg))] = ppmsg.RequestMsg
		p.height2Digest[ppmsg.SeqID] = string(getDigest(ppmsg.RequestMsg))

		//根据作恶IP进行作恶
		if p.ifSetMalicious && !p.lastBlockIfPartion {
			flag = p.setMaliciousAction()
		}
	}
	// if the message is true, broadcast the prepare message
	if flag {
		pre := message.Prepare{
			Digest:     ppmsg.Digest,
			SeqID:      ppmsg.SeqID,
			SenderNode: p.RunningNode,
		}
		prepareByte, err := json.Marshal(pre)
		if err != nil {
			log.Panic()
		}
		// broadcast
		msg_send := message.MergeMessage(message.CPrepare, prepareByte)
		networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
		networks.TcpDial(msg_send, p.RunningNode.IPaddr)
		p.pl.Plog.Printf("S%dN%d : has broadcast the prepare message \n", p.ShardID, p.NodeID)

		// Pbft stage add 1. It means that this round of pbft goes into the next stage, i.e., Prepare stage.
		p.pbftStage.Add(1)
	}
}

func (p *PbftConsensusNode) setMaliciousAction() bool {
	flag := true
	p.pl.Plog.Println("MaliciousIP传入:", p.maliciousIP)
	myPort, err := params.ExtractPortFromAddress(p.maliciousIP)
	if err != nil {
		p.pl.Plog.Println("提取作恶端口号出错:", err)
	} else {
		//此处设置怎样端口的节点作恶，目前是最后一个数字是3的节点。
		initialNodeID := myPort % 10
		initialShardID := (myPort - 28800 - initialNodeID) / 100
		if initialShardID%3 == 1 {
			if initialNodeID%4 == 3 { //p.NodeID != p.view &&
				//随机生成一个0-prob的数字，如果小于MaliciousProb则作恶，则作恶概率最低为MaliciousProb
				if params.MaliciousProb == 1.0 {
					flag = false
					return flag
				}
				prob := 100 - initialShardID*params.InitialShardProb - initialNodeID
				if float64(prob) < params.MaliciousProb*100 {
					prob = int(params.MaliciousProb * 100)
				}
				randomNumber := rand.Intn(prob)
				if float64(randomNumber)/100.0 < params.MaliciousProb {
					flag = false
				}
			}
		} else if initialShardID%3 == 2 {
			if initialNodeID%4 == 3 {
				if params.MaliciousProb == 1.0 {
					flag = false
					return flag
				}
				prob := 100 - initialShardID*params.InitialShardProb - initialNodeID
				if float64(prob) < params.MaliciousProb*100 {
					prob = int(params.MaliciousProb * 100)
				}
				randomNumber := rand.Intn(prob)
				if float64(randomNumber)/100.0 < params.MaliciousProb {
					flag = false
				}
			}
		}
	}
	return flag
}

func (p *PbftConsensusNode) setDelay() {
	//延迟发送prepare消息
	if p.ifSetDelay && !p.lastBlockIfPartion {
		myPort, err := params.ExtractPortFromAddress(p.maliciousIP)
		if err != nil {
			p.pl.Plog.Println("提取作恶端口号出错:", err)
		} else {
			//此处设置根据作恶IP设置相应延迟表示性能差异。
			initialNodeID := myPort % 10
			initialShardID := (myPort - 28800 - initialNodeID) / 100
			if initialNodeID != 0 {
				delayTime := time.Duration(initialShardID*params.ShardInitalDelay+initialNodeID*params.NodeInitalDelay) * time.Millisecond
				time.Sleep(delayTime)
				p.pl.Plog.Printf("S%dN%d :set delay %d ms\n", p.ShardID, p.NodeID, initialShardID*params.ShardInitalDelay+initialNodeID*params.NodeInitalDelay)
			}
		}
	}
}

// Handle prepare messages here.
// If you want to do more operations in the prepare stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinPrepare**
func (p *PbftConsensusNode) handlePrepare(content []byte) {
	p.pl.Plog.Printf("S%dN%d : received the Prepare ...\n", p.ShardID, p.NodeID)
	// decode the message
	pmsg := new(message.Prepare)
	err := json.Unmarshal(content, pmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	for p.pbftStage.Load() < 2 && pmsg.SeqID >= p.sequenceID && p.view.Load() == curView {
		p.conditionalVarpbftLock.Wait()
	}
	defer p.conditionalVarpbftLock.Broadcast()

	// if this message is out of date, return.
	if pmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	if _, ok := p.requestPool[string(pmsg.Digest)]; !ok {
		p.pl.Plog.Printf("S%dN%d : doesn't have the digest in the requst pool, refuse to commit\n", p.ShardID, p.NodeID)
	} else if p.sequenceID < pmsg.SeqID {
		p.pl.Plog.Printf("S%dN%d : inconsistent sequence ID, refuse to commit\n", p.ShardID, p.NodeID)
	} else {
		// if needed more operations, implement interfaces
		p.ihm.HandleinPrepare(pmsg)

		p.set2DMap(true, string(pmsg.Digest), pmsg.SenderNode)
		cnt := len(p.cntPrepareConfirm[string(pmsg.Digest)])

		// if the node has received 2f messages (itself included), and it haven't committed, then it commit
		p.lock.Lock()
		defer p.lock.Unlock()
		if uint64(cnt) >= 2*p.malicious_nums+1 && !p.isCommitBordcast[string(pmsg.Digest)] {
			p.pl.Plog.Printf("S%dN%d : is going to commit\n", p.ShardID, p.NodeID)
			// generate commit and broadcast
			c := message.Commit{
				Digest:     pmsg.Digest,
				SeqID:      pmsg.SeqID,
				SenderNode: p.RunningNode,
			}
			commitByte, err := json.Marshal(c)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.CCommit, commitByte)
			p.setDelay()
			networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
			networks.TcpDial(msg_send, p.RunningNode.IPaddr)
			p.isCommitBordcast[string(pmsg.Digest)] = true
			p.pl.Plog.Printf("S%dN%d : commit is broadcast\n", p.ShardID, p.NodeID)

			p.pbftStage.Add(1)
		}
	}
}

// Handle commit messages here.
// If you want to do more operations in the commit stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinCommit**
func (p *PbftConsensusNode) handleCommit(content []byte) {
	// decode the message
	cmsg := new(message.Commit)
	err := json.Unmarshal(content, cmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	for p.pbftStage.Load() < 3 && cmsg.SeqID >= p.sequenceID && p.view.Load() == curView {
		p.conditionalVarpbftLock.Wait()
	}
	defer p.conditionalVarpbftLock.Broadcast()

	if cmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	p.pl.Plog.Printf("S%dN%d received the Commit from ...%d\n", p.ShardID, p.NodeID, cmsg.SenderNode.NodeID)
	p.set2DMap(false, string(cmsg.Digest), cmsg.SenderNode)
	cnt := len(p.cntCommitConfirm[string(cmsg.Digest)])

	p.lock.Lock()
	defer p.lock.Unlock()

	if uint64(cnt) >= 2*p.malicious_nums+1 && !p.isReply[string(cmsg.Digest)] {
		p.pl.Plog.Printf("S%dN%d : has received 2f + 1 commits ... \n", p.ShardID, p.NodeID)
		// if this node is left behind, so it need to requst blocks
		if _, ok := p.requestPool[string(cmsg.Digest)]; !ok {
			p.isReply[string(cmsg.Digest)] = true
			p.askForLock.Lock()
			// request the block
			sn := &shard.Node{
				NodeID:  uint64(p.view.Load()),
				ShardID: p.ShardID,
				IPaddr:  p.ip_nodeTable[p.ShardID][uint64(p.view.Load())],
			}
			orequest := message.RequestOldMessage{
				SeqStartHeight: p.sequenceID + 1,
				SeqEndHeight:   cmsg.SeqID,
				ServerNode:     sn,
				SenderNode:     p.RunningNode,
			}
			bromyte, err := json.Marshal(orequest)
			if err != nil {
				log.Panic()
			}

			p.pl.Plog.Printf("S%dN%d : is now requesting message (seq %d to %d) ... \n", p.ShardID, p.NodeID, orequest.SeqStartHeight, orequest.SeqEndHeight)
			msg_send := message.MergeMessage(message.CRequestOldrequest, bromyte)
			p.setDelay()
			networks.TcpDial(msg_send, orequest.ServerNode.IPaddr)
		} else {
			if !p.lastBlockIfPartion {
				p.RecordSuccessBlockNodeAction(p.cntCommitConfirm[string(cmsg.Digest)])
			}
			// implement interface
			p.ihm.HandleinCommit(cmsg)
			p.isReply[string(cmsg.Digest)] = true
			p.pl.Plog.Printf("S%dN%d: this round of pbft %d is end \n", p.ShardID, p.NodeID, p.sequenceID)
			p.sequenceID += 1

			p.lastBlockIfSuccess = true
		}

		p.pbftStage.Store(1)
		p.lastCommitTime.Store(time.Now().UnixMilli())

		// if this node is a main node, then unlock the sequencelock
		if p.NodeID == uint64(p.view.Load()) {
			if !p.lastBlockIfPartion {
				p.SendNodeActionToSupervisor()
				p.pl.Plog.Printf("S%dN%d in pbft round %d sended nodeAction in my shard to supervisor ...\n", p.ShardID, p.NodeID, p.sequenceID)
			}
			p.sequenceLock.Unlock()
			p.pl.Plog.Printf("S%dN%d get sequenceLock unlocked...\n", p.ShardID, p.NodeID)
		}
	}
}

// 区块提交成功了，记录节点贡献交易、投票结果的函数
func (p *PbftConsensusNode) RecordSuccessBlockNodeAction(nodeIfCommit map[*shard.Node]bool) {
	neighborIfCommit := make(map[uint64]bool)
	for Node, ifCommit := range nodeIfCommit {
		if ifCommit { //hereeeeeeee
			if Node.NodeID != uint64(p.view.Load()) {
				if _, exist := p.nodeFR[Node.NodeID]; !exist {
					p.nodeFR[Node.NodeID] = 1
				} else {
					p.nodeFR[Node.NodeID]++
				}
			}
			neighborIfCommit[Node.NodeID] = true
		}
	}
	neighborIfCommit[uint64(p.view.Load())] = true
	if _, exist := p.nodeFR[uint64(p.view.Load())]; !exist {
		p.nodeMR[uint64(p.view.Load())] = 1
	} else {
		p.nodeMR[uint64(p.view.Load())]++
	}

	nodeRightNum := uint64(len(neighborIfCommit))
	//nodeWrongNum := p.node_nums - nodeRightNum
	for i := uint64(0); i < p.node_nums; i++ {
		if neighborIfCommit[i] {
			p.TXi[i] = p.TXi[i] + float32(p.txNumInBlock)/float32(nodeRightNum)
		} else {
			if i == uint64(p.view.Load()) {
				if _, exist := p.nodeFR[i]; !exist {
					p.nodeMW[i] = 1
				} else {
					p.nodeMW[i]++
				}
			} else {
				if _, exist := p.nodeFR[i]; !exist {
					p.nodeFW[i] = 1
				} else {
					p.nodeFW[i]++
				}
			}
			//此处不对未提交的节点进行处罚
		}
	}
	//对节点本epoch的安全贡献值进行更新
	for i := uint64(0); i < p.node_nums; i++ {
		numerator := params.Mu*(params.Lambda*float32(p.nodeMR[i])+float32(p.nodeFR[i])) - params.Theta*(params.Lambda*float32(p.nodeMW[i])+float32(p.nodeFW[i]))
		denominator := params.Lambda*(float32(p.nodeMR[i]+p.nodeMW[i])) + float32(p.nodeFR[i]+p.nodeFW[i])
		p.safeVauleInEpoch[i] = numerator / denominator
	}
}

// 发送节点最新的安全贡献值和累计贡献交易数给监督节点
func (p *PbftConsensusNode) SendNodeActionToSupervisor() {
	nodeAction := new(message.NodeAction)
	nodeAction.ShardIndex = p.ShardID
	//nodeAction.EpochIndex = p.CurChain.CurrentBlock.Header.Epoch
	nodeAction.SequenceIDInThisShard = p.sequenceID
	if nodeAction.SafeVauleInEpoch == nil {
		nodeAction.SafeVauleInEpoch = make(map[uint64]float32)
	}
	if nodeAction.TxinEpoch == nil {
		nodeAction.TxinEpoch = make(map[uint64]float32)
	}
	nodeAction.SafeVauleInEpoch = p.safeVauleInEpoch
	nodeAction.TxinEpoch = p.TXi
	nodeActionByte, err := json.Marshal(nodeAction)
	if err != nil {
		log.Panic()
	}
	msg_send := message.MergeMessage(message.CNodeAction, nodeActionByte)
	p.setDelay()
	go networks.TcpDial(msg_send, p.ip_nodeTable[params.SupervisorShard][0])
}

// this func is only invoked by the main node,
// if the request is correct, the main node will send
// block back to the message sender.
// now this function can send both block and partition
func (p *PbftConsensusNode) handleRequestOldSeq(content []byte) {
	if uint64(p.view.Load()) != p.NodeID {
		return
	}

	rom := new(message.RequestOldMessage)
	err := json.Unmarshal(content, rom)
	if err != nil {
		log.Panic()
	}
	p.pl.Plog.Printf("S%dN%d : received the old message requst from ...", p.ShardID, p.NodeID)
	rom.SenderNode.PrintNode()

	oldR := make([]*message.Request, 0)
	for height := rom.SeqStartHeight; height <= rom.SeqEndHeight; height++ {
		if _, ok := p.height2Digest[height]; !ok {
			p.pl.Plog.Printf("S%dN%d : has no this digest to this height %d\n", p.ShardID, p.NodeID, height)
			break
		}
		if r, ok := p.requestPool[p.height2Digest[height]]; !ok {
			p.pl.Plog.Printf("S%dN%d : has no this message to this digest %d\n", p.ShardID, p.NodeID, height)
			break
		} else {
			oldR = append(oldR, r)
		}
	}
	p.pl.Plog.Printf("S%dN%d : has generated the message to be sent\n", p.ShardID, p.NodeID)

	p.ihm.HandleReqestforOldSeq(rom)

	// send the block back
	sb := message.SendOldMessage{
		SeqStartHeight: rom.SeqStartHeight,
		SeqEndHeight:   rom.SeqEndHeight,
		OldRequest:     oldR,
		SenderNode:     p.RunningNode,
	}
	sbByte, err := json.Marshal(sb)
	if err != nil {
		log.Panic()
	}
	msg_send := message.MergeMessage(message.CSendOldrequest, sbByte)
	p.setDelay()
	networks.TcpDial(msg_send, rom.SenderNode.IPaddr)
	p.pl.Plog.Printf("S%dN%d : send blocks\n", p.ShardID, p.NodeID)
}

// node requst blocks and receive blocks from the main node
func (p *PbftConsensusNode) handleSendOldSeq(content []byte) {
	som := new(message.SendOldMessage)
	err := json.Unmarshal(content, som)
	if err != nil {
		log.Panic()
	}
	p.pl.Plog.Printf("S%dN%d : has received the SendOldMessage message\n", p.ShardID, p.NodeID)

	// implement interface for new consensus
	p.ihm.HandleforSequentialRequest(som)
	beginSeq := som.SeqStartHeight
	for idx, r := range som.OldRequest {
		p.requestPool[string(getDigest(r))] = r
		p.height2Digest[uint64(idx)+beginSeq] = string(getDigest(r))
		p.isReply[string(getDigest(r))] = true
		p.pl.Plog.Printf("this round of pbft %d is end \n", uint64(idx)+beginSeq)
	}
	p.sequenceID = som.SeqEndHeight + 1
	if rDigest, ok1 := p.height2Digest[p.sequenceID]; ok1 {
		if r, ok2 := p.requestPool[rDigest]; ok2 {
			ppmsg := &message.PrePrepare{
				RequestMsg: r,
				SeqID:      p.sequenceID,
				Digest:     getDigest(r),
			}
			flag := false
			flag = p.ihm.HandleinPrePrepare(ppmsg)
			if flag {
				pre := message.Prepare{
					Digest:     ppmsg.Digest,
					SeqID:      ppmsg.SeqID,
					SenderNode: p.RunningNode,
				}
				prepareByte, err := json.Marshal(pre)
				if err != nil {
					log.Panic()
				}
				// broadcast
				msg_send := message.MergeMessage(message.CPrepare, prepareByte)
				p.setDelay()
				networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
				p.pl.Plog.Printf("S%dN%d : has broadcast the prepare message \n", p.ShardID, p.NodeID)
			}
		}
	}

	p.askForLock.Unlock()
}

func (p *PbftConsensusNode) handleNodeMigration(content []byte) {
	// 解析节点迁移消息
	migrateMsg := new(message.NodeMigrationMsg)
	err := json.Unmarshal(content, migrateMsg)
	if err != nil {
		log.Panic(err)
	}

	// 先从所有分片删除该节点
	for _, nodeMap := range p.ip_nodeTable {
		delete(nodeMap, migrateMsg.NodeID)
	}
	// 加入新分片
	if _, ok := p.ip_nodeTable[migrateMsg.NewShard]; !ok {
		p.ip_nodeTable[migrateMsg.NewShard] = make(map[uint64]string)
	}
	p.ip_nodeTable[migrateMsg.NewShard][migrateMsg.NodeID] = migrateMsg.NewIP

	// 如果是本节点，还要更新自身信息
	if migrateMsg.NodeID == p.NodeID {
		p.RunningNode.IPaddr = migrateMsg.NewIP
		p.RunningNode.ShardID = migrateMsg.NewShard
		p.pl.Plog.Printf("Node %d migrated to new IP %s in shard %d\n", p.NodeID, p.RunningNode.IPaddr, p.RunningNode.ShardID)
	}
}
