package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const TimeWindow float64 = 1200.0

// RLPA committee operations
type RLPACommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	// additional variants
	curEpoch  int32
	rlpaLock  sync.Mutex
	rlpaGraph *partition.RLPAState
	// clpaLock    sync.Mutex
	// clpaGraph   *partition.CLPAState
	modifiedMap map[string]uint64
	// modifiedMap2        map[string]uint64
	rlpaLastRunningTime time.Time
	rlpaFreq            int

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string
}

func NewRLPACommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum, rlpaFrequency int) *RLPACommitteeModule {
	cg := new(partition.RLPAState)
	cg.Init_RLPAState(0.5, 10, params.ShardNum)

	// clg := new(partition.CLPAState)
	// clg.Init_CLPAState(0.5, 100, params.ShardNum)

	return &RLPACommitteeModule{
		csvPath:      csvFilePath,
		dataTotalNum: dataNum,
		batchDataNum: batchNum,
		nowDataNum:   0,
		rlpaGraph:    cg,
		// clpaGraph:    clg,
		modifiedMap: make(map[string]uint64),
		// modifiedMap2:        make(map[string]uint64),
		rlpaFreq:            rlpaFrequency,
		rlpaLastRunningTime: time.Time{},
		IpNodeTable:         Ip_nodeTable,
		Ss:                  Ss,
		sl:                  sl,
		curEpoch:            0,
	}
}

func (ccm *RLPACommitteeModule) HandleOtherMessage([]byte) {}

func (ccm *RLPACommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := ccm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (ccm *RLPACommitteeModule) txSending(txlist []*core.Transaction) {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// send to shard
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				go networks.TcpDial(send_msg, ccm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		sendersid := ccm.fetchModifiedMap(tx.Sender)
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
}

func (ccm *RLPACommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(ccm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)
	rlpaCnt := 0
	flag := true
	// clpaCnt := 0
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}

		if tx, ok := data2tx(data, uint64(ccm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			ccm.nowDataNum++
		} else {
			continue
		}
		// batch sending condition
		if len(txlist) == int(ccm.batchDataNum) || ccm.nowDataNum == ccm.dataTotalNum {
			// set the algorithm timer begins
			if ccm.rlpaLastRunningTime.IsZero() {
				ccm.rlpaLastRunningTime = time.Now()
			}
			ccm.txSending(txlist)

			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			ccm.Ss.StopGap_Reset()
		}

		if params.ShardNum > 1 && !ccm.rlpaLastRunningTime.IsZero() && time.Since(ccm.rlpaLastRunningTime) >= time.Duration(0.5*float64(ccm.rlpaFreq))*time.Second && time.Since(ccm.rlpaLastRunningTime) < time.Duration(ccm.rlpaFreq)*time.Second && flag {
			ccm.rlpaLock.Lock()
			rlpaCnt++
			mmap, _ := ccm.rlpaGraph.RLPA_Partition()

			ccm.rlpaMapSend(mmap)
			for key, val := range mmap {
				ccm.modifiedMap[key] = val
			}
			ccm.rlpaReset()
			ccm.rlpaLock.Unlock()
			flag = false
			ccm.sl.Slog.Println("Run RLPA Duration ")
			if ccm.nowDataNum == ccm.dataTotalNum {
				break
			}
		}

		if params.ShardNum > 1 && !ccm.rlpaLastRunningTime.IsZero() && time.Since(ccm.rlpaLastRunningTime) >= time.Duration(ccm.rlpaFreq)*time.Second {
			ccm.rlpaLock.Lock()
			rlpaCnt++
			mmap, _ := ccm.rlpaGraph.RLPA_Partition()

			ccm.rlpaMapSend(mmap)
			for key, val := range mmap {
				ccm.modifiedMap[key] = val
			}
			ccm.rlpaReset()
			ccm.rlpaLock.Unlock()

			// ccm.clpaLock.Lock()
			// clpaCnt++
			// mmap2, _ := ccm.clpaGraph.CLPA_Partition()

			// ccm.clpaMapSend(mmap2)
			// for key, val := range mmap2 {
			// 	ccm.modifiedMap2[key] = val
			// }
			// ccm.clpaReset()
			// ccm.clpaLock.Unlock()

			for atomic.LoadInt32(&ccm.curEpoch) != int32(rlpaCnt) {
				time.Sleep(time.Second)
			}

			ccm.rlpaLastRunningTime = time.Now()
			flag = true
			ccm.sl.Slog.Println("Next RLPA epoch begins. ")
		}

		if ccm.nowDataNum == ccm.dataTotalNum {
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !ccm.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		if time.Since(ccm.rlpaLastRunningTime) >= time.Duration(ccm.rlpaFreq)*time.Second {
			ccm.rlpaLock.Lock()
			rlpaCnt++
			mmap, _ := ccm.rlpaGraph.RLPA_Partition()
			ccm.rlpaMapSend(mmap)
			for key, val := range mmap {
				ccm.modifiedMap[key] = val
			}
			ccm.rlpaReset()
			ccm.rlpaLock.Unlock()

			// ccm.clpaLock.Lock()
			// clpaCnt++
			// rlpaCnt++
			// mmap, _ := ccm.clpaGraph.CLPA_Partition()
			// ccm.clpaMapSend(mmap)
			// for key, val := range mmap {
			// 	ccm.modifiedMap[key] = val
			// }
			// ccm.clpaReset()
			// ccm.clpaLock.Unlock()
			for atomic.LoadInt32(&ccm.curEpoch) != int32(rlpaCnt) {
				time.Sleep(time.Second)
			}
			// for atomic.LoadInt32(&ccm.curEpoch) != int32(clpaCnt) {
			// 	time.Sleep(time.Second)
			// }
			ccm.sl.Slog.Printf("Current Epoch: %d, RLPA Count: %d\n", ccm.curEpoch, rlpaCnt)
			ccm.sl.Slog.Println("Next RLPA epoch begins. ")
			ccm.rlpaLastRunningTime = time.Now()
		}
	}
}

func (ccm *RLPACommitteeModule) rlpaMapSend(m map[string]uint64) {
	// send partition modified Map message
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
	}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionMsg, pmByte)
	// send to worker shards
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go networks.TcpDial(send_msg, ccm.IpNodeTable[i][0])
	}
	ccm.sl.Slog.Println("Supervisor: all partition map message has been sent. ")
}

// func (ccm *RLPACommitteeModule) clpaMapSend(m map[string]uint64) {
// 	// send partition modified Map message
// 	pm := message.PartitionModifiedMap{
// 		PartitionModified: m,
// 	}
// 	pmByte, err := json.Marshal(pm)
// 	if err != nil {
// 		log.Panic()
// 	}
// 	send_msg := message.MergeMessage(message.CPartitionMsg, pmByte)
// 	// send to worker shards
// 	for i := uint64(0); i < uint64(params.ShardNum); i++ {
// 		go networks.TcpDial(send_msg, ccm.IpNodeTable[i][0])
// 	}
// 	ccm.sl.Slog.Println("Supervisor: all partition map message has been sent. ")
// }

func (ccm *RLPACommitteeModule) rlpaReset() {
	ccm.rlpaGraph = new(partition.RLPAState)
	ccm.rlpaGraph.Init_RLPAState(0.5, 100, params.ShardNum)
	for key, val := range ccm.modifiedMap {
		ccm.rlpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
	// ccm.rlpaGraph.PartitionMap = make(map[partition.Vertex]int)
	// ccm.rlpaGraph.Edges2Shard = make([]int, ccm.rlpaGraph.ShardNum)
}

// func (ccm *RLPACommitteeModule) clpaReset() {
// 	ccm.clpaGraph = new(partition.CLPAState)
// 	ccm.clpaGraph.Init_CLPAState(0.5, 100, params.ShardNum)
// 	for key, val := range ccm.modifiedMap {
// 		ccm.clpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
// 	}
// }

func (ccm *RLPACommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	ccm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	if atomic.CompareAndSwapInt32(&ccm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		ccm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}
	if b.BlockBodyLength == 0 {
		return
	}
	ccm.rlpaLock.Lock()
	for _, tx := range b.InnerShardTxs {
		ccm.rlpaGraph.UpdateAccountFrequency(tx.Sender, tx.Recipient)

		// 检查是否为热点账户
		if ccm.rlpaGraph.IsHotAccount(tx.Sender) || ccm.rlpaGraph.IsHotAccount(tx.Recipient) {
			ccm.rlpaGraph.AddEdgeWithTime(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient}, float64(tx.Time.Unix()), TimeWindow)
		}
	}
	for _, r2tx := range b.Relay2Txs {
		ccm.rlpaGraph.UpdateAccountFrequency(r2tx.Sender, r2tx.Recipient)

		// 检查是否为热点账户
		if ccm.rlpaGraph.IsHotAccount(r2tx.Sender) || ccm.rlpaGraph.IsHotAccount(r2tx.Recipient) {
			ccm.rlpaGraph.AddEdgeWithTime(partition.Vertex{Addr: r2tx.Sender}, partition.Vertex{Addr: r2tx.Recipient}, float64(r2tx.Time.Unix()), TimeWindow)
		}

	}
	// ccm.clpaLock.Unlock()
	ccm.rlpaLock.Unlock()
}
