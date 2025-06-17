// The pbft consensus process

package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/shard"
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

// const TimeWindow int64 = 60

type PbftConsensusNode struct {
	// the local config about pbft
	RunningNode *shard.Node // the node information
	ShardID     uint64      // denote the ID of the shard (or pbft), only one pbft consensus in a shard
	NodeID      uint64      // denote the ID of the node in the pbft (shard)

	// the data structure for blockchain
	CurChain *chain.BlockChain // all node in the shard maintain the same blockchain
	db       ethdb.Database    // to save the mpt

	// the global config about pbft
	pbftChainConfig *params.ChainConfig          // the chain config in this pbft
	ip_nodeTable    map[uint64]map[uint64]string // denote the ip of the specific node
	node_nums       uint64                       // the number of nodes in this pfbt, denoted by N
	malicious_nums  uint64                       // f, 3f + 1 = N

	// view change
	view           atomic.Int32 // denote the view of this pbft, the main node can be inferred from this variant
	lastCommitTime atomic.Int64 // the time since last commit.
	viewChangeMap  map[ViewChangeData]map[uint64]bool
	newViewMap     map[ViewChangeData]map[uint64]bool

	// the control message and message checking utils in pbft
	sequenceID        uint64                          // the message sequence id of the pbft
	stopSignal        atomic.Bool                     // send stop signal
	pStop             chan uint64                     // channle for stopping consensus
	requestPool       map[string]*message.Request     // RequestHash to Request
	cntPrepareConfirm map[string]map[*shard.Node]bool // count the prepare confirm message, [messageHash][Node]bool
	cntCommitConfirm  map[string]map[*shard.Node]bool // count the commit confirm message, [messageHash][Node]bool
	isCommitBordcast  map[string]bool                 // denote whether the commit is broadcast
	isReply           map[string]bool                 // denote whether the message is reply
	height2Digest     map[uint64]string               // sequence (block height) -> request, fast read

	// pbft stage wait
	pbftStage              atomic.Int32 // 1->Preprepare, 2->Prepare, 3->Commit, 4->Done
	pbftLock               sync.Mutex
	conditionalVarpbftLock sync.Cond

	// locks about pbft
	sequenceLock sync.Mutex // the lock of sequence
	lock         sync.Mutex // lock the stage
	askForLock   sync.Mutex // lock for asking for a serise of requests

	// seqID of other Shards, to synchronize
	seqIDMap   map[uint64]uint64
	seqMapLock sync.Mutex

	// logger
	pl *pbft_log.PbftLog
	// tcp control
	tcpln       net.Listener
	tcpPoolLock sync.Mutex

	// to handle the message in the pbft
	ihm ExtraOpInConsensus

	// to handle the message outside of pbft
	ohm OpInterShards

	// 节点贡献值记录
	//添加变量，记录所在分片内各节点的作为主节点、从节点的正确、错误行为数，
	//即$N_{MR_i}、N_{FR_i}、N_{MW_i}、N_{FW_i}$，其中$i$为节点编号
	//同时记录所在分片内各节点在epoch内的累计贡献交易$TX_j^i$
	//即$TX_{j}^i$，其中$j$为区块编号，包括成功提交和失败提交的区块
	nodeMR           map[uint64]uint64
	nodeFR           map[uint64]uint64
	nodeMW           map[uint64]uint64
	nodeFW           map[uint64]uint64
	safeVauleInEpoch map[uint64]float32 //记录每个节点在epoch内的安全贡献值变化
	txNumInBlock     uint64             //记录上一个区块的交易数
	TXi              map[uint64]float32 //记录每个节点的累计贡献交易数
	//perforVauleInEpoch map[uint64]uint64  //记录每个节点在epoch内的性能贡献值变化

	lastDigest         string //上一个区块的消息摘要
	lastBlockIfSuccess bool   //上一个区块是否成功提交
	lastBlockIfPartion bool   //上一个区块是否分片

	ifSetMalicious bool   //是否可能作恶
	ifSetDelay     bool   //是否可能延迟
	maliciousIP    string //作恶IP

	// 用于存储跨分片交易的时间信息
	// TransactionTimes map[string][]int64 // key: "sender->recipient", value: list of transaction timestamps
	NetGraph *partition.RGraph // 图结构，用于存储节点和边
}

// generate a pbft consensus for a node
func NewPbftNode(shardID, nodeID uint64, pcc *params.ChainConfig, messageHandleType string) *PbftConsensusNode {
	p := new(PbftConsensusNode)
	p.ip_nodeTable = params.IPmap_nodeTable
	p.node_nums = pcc.Nodes_perShard
	p.ShardID = shardID
	p.NodeID = nodeID
	p.pbftChainConfig = pcc
	// p.TransactionTimes = make(map[string][]int64)
	// 初始化 NetGraph
	p.NetGraph = &partition.RGraph{
		VertexSet: make(map[partition.Vertex]bool),
		EdgeSet:   make(map[partition.Vertex]map[partition.Vertex]float64),
		EdgeTimes: make(map[partition.Vertex]map[partition.Vertex][]float64),
	}
	fp := params.DatabaseWrite_path + "mptDB/ldb/s" + strconv.FormatUint(shardID, 10) + "/n" + strconv.FormatUint(nodeID, 10)
	var err error
	p.db, err = rawdb.NewLevelDBDatabase(fp, 0, 1, "accountState", false)
	if err != nil {
		log.Panic(err)
	}
	p.CurChain, err = chain.NewBlockChain(pcc, p.db)
	if err != nil {
		log.Panic("cannot new a blockchain")
	}

	p.RunningNode = &shard.Node{
		NodeID:  nodeID,
		ShardID: shardID,
		IPaddr:  p.ip_nodeTable[shardID][nodeID],
	}

	p.stopSignal.Store(false)
	p.sequenceID = p.CurChain.CurrentBlock.Header.Number + 1
	p.pStop = make(chan uint64)
	p.requestPool = make(map[string]*message.Request)
	p.cntPrepareConfirm = make(map[string]map[*shard.Node]bool)
	p.cntCommitConfirm = make(map[string]map[*shard.Node]bool)
	p.isCommitBordcast = make(map[string]bool)
	p.isReply = make(map[string]bool)
	p.height2Digest = make(map[uint64]string)
	p.malicious_nums = (p.node_nums - 1) / 3

	// init view & last commit time
	p.view.Store(0)
	p.lastCommitTime.Store(time.Now().Add(time.Second * 5).UnixMilli())
	p.viewChangeMap = make(map[ViewChangeData]map[uint64]bool)
	p.newViewMap = make(map[ViewChangeData]map[uint64]bool)

	p.seqIDMap = make(map[uint64]uint64)

	p.pl = pbft_log.NewPbftLog(shardID, nodeID)

	// 节点贡献值记录
	p.nodeMR = make(map[uint64]uint64)
	p.nodeFR = make(map[uint64]uint64)
	p.nodeMW = make(map[uint64]uint64)
	p.nodeFW = make(map[uint64]uint64)
	p.TXi = make(map[uint64]float32)
	p.safeVauleInEpoch = make(map[uint64]float32)
	//p.perforVauleInEpoch = make(map[uint64]uint64)
	p.txNumInBlock = 0
	p.lastDigest = ""
	p.lastBlockIfSuccess = false
	p.lastBlockIfPartion = false
	p.ifSetMalicious = params.IfSetMalicious
	p.ifSetDelay = params.IfSetDelay
	p.maliciousIP = p.ip_nodeTable[shardID][nodeID]

	// choose how to handle the messages in pbft or beyond pbft
	switch string(messageHandleType) {
	case "CLPA_Broker":
		ncdm := dataSupport.NewCLPADataSupport()
		p.ihm = &CLPAPbftInsideExtraHandleMod_forBroker{
			pbftNode: p,
			cdm:      ncdm,
		}
		p.ohm = &CLPABrokerOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	case "CLPA":
		ncdm := dataSupport.NewCLPADataSupport()
		p.ihm = &CLPAPbftInsideExtraHandleMod{
			pbftNode: p,
			cdm:      ncdm,
		}
		p.ohm = &CLPARelayOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	case "RLPA":
		ncdm := dataSupport.NewRLPADataSupport()
		p.ihm = &RLPAPbftInsideExtraHandleMod{
			pbftNode: p,
			cdm:      ncdm,
			epochID:  0,
		}
		p.ohm = &RLPARelayOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	case "Broker":
		p.ihm = &RawBrokerPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawBrokerOutsideModule{
			pbftNode: p,
		}
	case "P-Louvain":
		ncdm := dataSupport.NewCLPADataSupport()
		p.ihm = &PLouvainPbftInsideExtraHandleMod{
			pbftNode: p,
			cdm:      ncdm,
			epochID:  0,
		}
		p.ohm = &PLouvainRelayOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	default:
		p.ihm = &RawRelayPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawRelayOutsideModule{
			pbftNode: p,
		}
	}

	// set pbft stage now
	p.conditionalVarpbftLock = *sync.NewCond(&p.pbftLock)
	p.pbftStage.Store(1)

	return p
}

// handle the raw message, send it to corresponded interfaces
func (p *PbftConsensusNode) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	// pbft inside message type
	case message.CPrePrepare:
		// use "go" to start a go routine to handle this message, so that a pre-arrival message will not be aborted.
		go p.handlePrePrepare(content)
	case message.CPrepare:
		// use "go" to start a go routine to handle this message, so that a pre-arrival message will not be aborted.
		go p.handlePrepare(content)
	case message.CCommit:
		// use "go" to start a go routine to handle this message, so that a pre-arrival message will not be aborted.
		go p.handleCommit(content)
	// case message.CrossShardTransaction:
	// 	go p.handleCrossShardTransaction(content)
	case message.ViewChangePropose:
		p.handleViewChangeMsg(content)
	case message.NewChange:
		p.handleNewViewMsg(content)

	case message.CRequestOldrequest:
		p.handleRequestOldSeq(content)
	case message.CSendOldrequest:
		p.handleSendOldSeq(content)
	case message.CNodeMigration:
		p.handleNodeMigration(content)
	case message.NodeAllocMsg:
		p.clearActionRecord()
		p.handleNodeAlloc(content)

	case message.CStop:
		p.WaitToStop()

	// handle the message from outside
	default:
		go p.ohm.HandleMessageOutsidePBFT(msgType, content)
	}
}

// 清除本地贡献交易、行为记录
func (p *PbftConsensusNode) clearActionRecord() {
	p.nodeMR = make(map[uint64]uint64)
	p.nodeFR = make(map[uint64]uint64)
	p.nodeMW = make(map[uint64]uint64)
	p.nodeFW = make(map[uint64]uint64)
	p.TXi = make(map[uint64]float32)
	p.safeVauleInEpoch = make(map[uint64]float32)
}

// when the leader received the nodeNewMap message, it should update the local nodeNewMap
func (p *PbftConsensusNode) handleNodeAlloc(content []byte) {
	nnm := new(message.NodeAllocResult)
	err := json.Unmarshal(content, nnm)
	if err != nil {
		log.Panic()
	}
	p.pl.Plog.Println("NodeMaliciousIP数组：", nnm.NodeMaliciousIP)
	p.maliciousIP = nnm.NodeMaliciousIP[p.RunningNode.IPaddr]
	p.ihm.UpdateEpochID(nnm.EpochID)
	p.pl.Plog.Printf("S%dN%d : has received epoch %d nodeAlloc message and change my maliciousIP\n", p.ShardID, p.NodeID, nnm.EpochID)
}

func (p *PbftConsensusNode) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		if p.stopSignal.Load() {
			return
		}
		switch err {
		case nil:
			p.tcpPoolLock.Lock()
			p.handleMessage(clientRequest)
			p.tcpPoolLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

// A consensus node starts tcp-listen.
func (p *PbftConsensusNode) TcpListen() {
	ln, err := net.Listen("tcp", p.RunningNode.IPaddr)
	p.tcpln = ln
	if err != nil {
		log.Panic(err)
	}
	for {
		conn, err := p.tcpln.Accept()
		if err != nil {
			return
		}
		go p.handleClientRequest(conn)
	}
}

// When receiving a stop message, this node try to stop.
func (p *PbftConsensusNode) WaitToStop() {
	p.pl.Plog.Println("handling stop message")
	p.stopSignal.Store(true)
	networks.CloseAllConnInPool()
	p.tcpln.Close()
	p.closePbft()
	p.pl.Plog.Println("handled stop message in TCPListen Routine")
	p.pStop <- 1
}

// close the pbft
func (p *PbftConsensusNode) closePbft() {
	p.CurChain.CloseBlockChain()
}
