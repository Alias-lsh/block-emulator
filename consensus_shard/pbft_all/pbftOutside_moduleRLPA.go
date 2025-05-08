package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/message"
	"encoding/json"
	"log"
)

// This module used in the blockChain using transaction relaying mechanism.
// "RLPA" means that the blockChain use Account State Transfer protocal by rlpa.
type RLPARelayOutsideModule struct {
	cdm      *dataSupport.Data_supportRLPA
	pbftNode *PbftConsensusNode
}

func (crom *RLPARelayOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CRelay:
		crom.handleRelay(content)
	case message.CRelayWithProof:
		crom.handleRelayWithProof(content)
	case message.CInject:
		crom.handleInjectTx(content)

	// messages about RLPA
	case message.CPartitionMsg:
		crom.handlePartitionMsg(content)
	case message.AccountState_and_TX:
		crom.handleAccountStateAndTxMsg(content)
	case message.CPartitionReady:
		crom.handlePartitionReady(content)
	default:
	}
	return true
}

// receive relay transaction, which is for cross shard txs
func (crom *RLPARelayOutsideModule) handleRelay(content []byte) {
	relay := new(message.Relay)
	err := json.Unmarshal(content, relay)
	if err != nil {
		log.Panic(err)
	}

	// // 更新关联度
	// for _, tx := range relay.Txs {
	// 	// // 更新账户交易频率
	// 	// crom.cdm.UpdateAccountFrequency(tx.Sender, tx.Recipient)
	// 	// 如果交易涉及热点账户，则更新图
	// 	if crom.cdm.IsHotAccount(tx.Sender) || crom.cdm.IsHotAccount(tx.Recipient) {
	// 		// 获取当前时间
	// 		t_now := time.Now().Unix()
	// 		sender := partition.Vertex{Addr: tx.Sender}
	// 		recipient := partition.Vertex{Addr: tx.Recipient}
	// 		crom.pbftNode.NetGraph.AddEdgeWithTime(sender, recipient, t_now, TimeWindow)
	// 	}

	// 	// sender := partition.Vertex{Addr: tx.Sender}
	// 	// recipient := partition.Vertex{Addr: tx.Recipient}
	// 	// crom.pbftNode.NetGraph.AddEdgeWithTime(sender, recipient, t_now, TimeWindow)
	// }

	crom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs from shard %d, the senderSeq is %d\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, relay.SenderShardID, relay.SenderSeq)
	crom.pbftNode.CurChain.Txpool.AddTxs2Pool(relay.Txs)
	crom.pbftNode.seqMapLock.Lock()
	crom.pbftNode.seqIDMap[relay.SenderShardID] = relay.SenderSeq
	crom.pbftNode.seqMapLock.Unlock()
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
}

func (crom *RLPARelayOutsideModule) handleRelayWithProof(content []byte) {
	rwp := new(message.RelayWithProof)
	err := json.Unmarshal(content, rwp)
	if err != nil {
		log.Panic(err)
	}
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs & proofs from shard %d, the senderSeq is %d\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, rwp.SenderShardID, rwp.SenderSeq)
	// validate the proofs of txs
	isAllCorrect := true
	for i, tx := range rwp.Txs {
		if ok, _ := chain.TxProofVerify(tx.TxHash, &rwp.TxProofs[i]); !ok {
			isAllCorrect = false
			break
		}
	}
	if isAllCorrect {
		crom.pbftNode.CurChain.Txpool.AddTxs2Pool(rwp.Txs)
	} else {
		crom.pbftNode.pl.Plog.Println("Err: wrong proof!")
	}

	crom.pbftNode.seqMapLock.Lock()
	crom.pbftNode.seqIDMap[rwp.SenderShardID] = rwp.SenderSeq
	crom.pbftNode.seqMapLock.Unlock()
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
}

func (crom *RLPARelayOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	// for _, tx := range it.Txs {
	// 	// 更新账户交易频率
	// 	crom.cdm.UpdateAccountFrequency(tx.Sender, tx.Recipient)
	// }
	crom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, len(it.Txs))
}

// the leader received the partition message from listener/decider,
// it init the local variant and send the accout message to other leaders.
func (crom *RLPARelayOutsideModule) handlePartitionMsg(content []byte) {
	pm := new(message.PartitionModifiedMap)
	err := json.Unmarshal(content, pm)
	if err != nil {
		log.Panic()
	}
	crom.cdm.ModifiedMap = append(crom.cdm.ModifiedMap, pm.PartitionModified)
	crom.pbftNode.pl.Plog.Printf("S%dN%d : has received partition message\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
	crom.cdm.PartitionOn = true
}

// wait for other shards' last rounds are over
func (crom *RLPARelayOutsideModule) handlePartitionReady(content []byte) {
	pr := new(message.PartitionReady)
	err := json.Unmarshal(content, pr)
	if err != nil {
		log.Panic()
	}
	crom.cdm.P_ReadyLock.Lock()
	crom.cdm.PartitionReady[pr.FromShard] = true
	crom.cdm.P_ReadyLock.Unlock()

	crom.pbftNode.seqMapLock.Lock()
	crom.cdm.ReadySeq[pr.FromShard] = pr.NowSeqID
	crom.pbftNode.seqMapLock.Unlock()

	crom.pbftNode.pl.Plog.Printf("ready message from shard %d, seqid is %d\n", pr.FromShard, pr.NowSeqID)
}

// when the message from other shard arriving, it should be added into the message pool
func (crom *RLPARelayOutsideModule) handleAccountStateAndTxMsg(content []byte) {
	at := new(message.AccountStateAndTx)
	err := json.Unmarshal(content, at)
	if err != nil {
		log.Panic()
	}
	crom.cdm.AccountStateTx[at.FromShard] = at
	crom.pbftNode.pl.Plog.Printf("S%dN%d has added the accoutStateandTx from %d to pool\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID, at.FromShard)

	if len(crom.cdm.AccountStateTx) == int(crom.pbftNode.pbftChainConfig.ShardNums)-1 {
		crom.cdm.CollectLock.Lock()
		crom.cdm.CollectOver = true
		crom.cdm.CollectLock.Unlock()
		crom.pbftNode.pl.Plog.Printf("S%dN%d has added all accoutStateandTx~~~\n", crom.pbftNode.ShardID, crom.pbftNode.NodeID)
	}
}
