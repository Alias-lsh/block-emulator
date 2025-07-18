package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/nodeAllocate"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/exp/rand"
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
	shardPerformance    []float32

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string

	nodeValueHistory     *NodeValueHistory
	nodeAllocLastRunTime time.Time
	nodeAllocFreq        int
	shardLoadHistory     map[uint64][]float64

	epochId int
}

type NodeValueHistory struct {
	// epoch内节点贡献值变化、贡献交易变化
	nodeSafeVaule            map[uint64]map[uint64]float32 //分片id，节点id，节点贡献值
	temSafeVaule             map[uint64]map[uint64]float32 //epoch内分片id，节点id，节点贡献值
	nodePerformanceVaule     map[uint64]map[uint64]float32 //分片id，节点id，节点性能值
	temPerformanceVaule      map[uint64]map[uint64]float32 //epoch内分片id，节点id，节点性能值
	nodePerformanceVauleNorm map[uint64]map[uint64]float32 // 只存归一化结果
	nodeSafeVauleNorm        map[uint64]map[uint64]float32 // 只存归一化结果
}

func (nah *NodeValueHistory) Init_NodeValueHistory(Ip_nodeTable map[uint64]map[uint64]string) {
	//初始化节点安全、性能贡献值
	nah.nodeSafeVaule = make(map[uint64]map[uint64]float32)
	nah.nodePerformanceVaule = make(map[uint64]map[uint64]float32)
	nah.temSafeVaule = make(map[uint64]map[uint64]float32)
	nah.temPerformanceVaule = make(map[uint64]map[uint64]float32)
	nah.nodePerformanceVauleNorm = make(map[uint64]map[uint64]float32)
	nah.nodeSafeVauleNorm = make(map[uint64]map[uint64]float32)
	for i := uint64(0); i < uint64(len(Ip_nodeTable)-1); i++ {
		nah.nodeSafeVaule[i] = make(map[uint64]float32)
		nah.nodePerformanceVaule[i] = make(map[uint64]float32)
		nah.temSafeVaule[i] = make(map[uint64]float32)
		nah.temPerformanceVaule[i] = make(map[uint64]float32)
		nah.nodePerformanceVauleNorm[i] = make(map[uint64]float32)
		nah.nodeSafeVauleNorm[i] = make(map[uint64]float32)
		for j := uint64(0); j < uint64(len(Ip_nodeTable[i])); j++ {
			nah.nodeSafeVaule[i][j] = 0.5
			nah.nodePerformanceVaule[i][j] = 0.0
			nah.nodePerformanceVauleNorm[i][j] = 0.0
			nah.nodeSafeVauleNorm[i][j] = 0.0
		}
	}
}

func NewRLPACommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum, rlpaFrequency int, shardPerformance []float32) *RLPACommitteeModule {
	cg := new(partition.RLPAState)
	cg.Init_RLPAState(params.Beta, shardPerformance, 0.5, 10, params.ShardNum)

	// clg := new(partition.CLPAState)
	// clg.Init_CLPAState(0.5, 100, params.ShardNum)
	newnodeValueHistory := new(NodeValueHistory)
	newnodeValueHistory.Init_NodeValueHistory(Ip_nodeTable)
	shardLoad := make(map[uint64][]float64)
	return &RLPACommitteeModule{
		csvPath:      csvFilePath,
		dataTotalNum: dataNum,
		batchDataNum: batchNum,
		nowDataNum:   0,
		rlpaGraph:    cg,
		// clpaGraph:    clg,
		modifiedMap: make(map[string]uint64),
		// modifiedMap2:        make(map[string]uint64),
		rlpaFreq:             rlpaFrequency,
		rlpaLastRunningTime:  time.Time{},
		IpNodeTable:          Ip_nodeTable,
		Ss:                   Ss,
		sl:                   sl,
		curEpoch:             0,
		nodeValueHistory:     newnodeValueHistory,
		nodeAllocLastRunTime: time.Time{},
		nodeAllocFreq:        params.NodeAllocFreq,
		shardLoadHistory:     shardLoad,
		epochId:              0,
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

// 监督者处理节点动作消息，更新epoch内节点贡献值变化、贡献交易变化
func (ccm *RLPACommitteeModule) HandleNodeAction(content []byte) {
	na := new(message.NodeAction)
	na.SafeVauleInEpoch = make(map[uint64]float32)
	na.TxinEpoch = make(map[uint64]float32)

	err := json.Unmarshal(content, na)
	if err != nil {
		ccm.sl.Slog.Printf("Supervisor: json.Unmarshal error: %v\n", err)
		log.Panic(err)
	}
	ccm.sl.Slog.Printf("Supervisor: begins update node value using nodeAction message.\n")
	//epoch节点贡献值更新
	//打印节点贡献值
	for nodeID, safeValue := range na.SafeVauleInEpoch {
		ccm.sl.Slog.Printf("Supervisor: shard %d node %d safe value is %f\n", na.ShardIndex, nodeID, safeValue)
		ccm.nodeValueHistory.temSafeVaule[na.ShardIndex][nodeID] = safeValue
	}
	for nodeID, txValue := range na.TxinEpoch {
		ccm.sl.Slog.Printf("Supervisor: shard %d node %d tx value is %f\n", na.ShardIndex, nodeID, txValue)
		ccm.nodeValueHistory.temPerformanceVaule[na.ShardIndex][nodeID] = txValue
	}
	ccm.sl.Slog.Printf("Supervisor: have updated node value using nodeAction message.\n")
}

func (ccm *RLPACommitteeModule) updateNodeValue(timeDurtionInEpoch time.Duration, epochId int) {
	ccm.sl.Slog.Printf("Supervisor: epoch %d update NodeValue...", epochId)
	duration := timeDurtionInEpoch * time.Nanosecond
	// 将time.Duration转换为秒，并转换为uint64
	timeInEpoch := uint64(duration.Nanoseconds()) / uint64(time.Second)
	if epochId == 0 {
		ccm.nodeValueHistory.nodeSafeVaule = ccm.nodeValueHistory.temSafeVaule
		for shardID, nodeAction := range ccm.nodeValueHistory.temPerformanceVaule {
			for nodeID, txValue := range nodeAction {
				initialValue := txValue / float32(timeInEpoch)
				ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID] = initialValue
				ccm.nodeValueHistory.temPerformanceVaule[shardID][nodeID] = ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID]
			}
		}
	} else {
		// 根据本epoch贡献值改变量更新节点全局的安全贡献值
		for shardID, nodeAction := range ccm.nodeValueHistory.temSafeVaule {
			for nodeID, safeValue := range nodeAction {
				oldVaule := ccm.nodeValueHistory.nodeSafeVaule[shardID][nodeID]
				ccm.nodeValueHistory.nodeSafeVaule[shardID][nodeID] = params.Alpha*oldVaule + (1-params.Alpha)*safeValue
				ccm.nodeValueHistory.temSafeVaule[shardID][nodeID] = ccm.nodeValueHistory.nodeSafeVaule[shardID][nodeID]
			}
		}
		// 更新节点全局的性能贡献值
		for shardID, nodeAction := range ccm.nodeValueHistory.temPerformanceVaule {
			for nodeID, txValue := range nodeAction {
				oldVaule := ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID]
				changeVaule := txValue / float32(timeInEpoch)
				ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID] = params.Alpha*oldVaule + (1-params.Alpha)*changeVaule
				ccm.nodeValueHistory.temPerformanceVaule[shardID][nodeID] = ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID]
			}
		}
	}
	//更新分片性能值
	ccm.shardPerformance = ccm.calculateShardPerformance()
	ccm.NormalizeNodePerformance()
	ccm.NormalizeNodeSafeValue()
}

// 根据节点安全、性能贡献值计算分片性能值
func (ccm *RLPACommitteeModule) calculateShardPerformance() []float32 {
	shardPerformance := make([]float32, len(ccm.nodeValueHistory.nodePerformanceVaule))
	for i := 0; i < len(ccm.nodeValueHistory.nodePerformanceVaule); i++ {
		shardPerformance[i] = 0
		for j := 0; j < len(ccm.nodeValueHistory.nodePerformanceVaule[uint64(i)]); j++ {
			shardPerformance[i] += ccm.nodeValueHistory.nodePerformanceVaule[uint64(i)][uint64(j)]
		}
	}
	return shardPerformance
}

func (ccm *RLPACommitteeModule) updateValueAfterMove(newIptable map[uint64]map[uint64]string) {
	//更新节点贡献值记录
	ipSafeValue := make(map[string]float32)
	iptemSafeValue := make(map[string]float32)
	ipPerformanceValue := make(map[string]float32)
	iptemPerformanceValue := make(map[string]float32)
	for shardID, nodelist := range ccm.IpNodeTable {
		if shardID == params.SupervisorShard {
			continue
		}
		for nodeID, ip := range nodelist {
			ipSafeValue[ip] = ccm.nodeValueHistory.nodeSafeVaule[shardID][nodeID]
			iptemSafeValue[ip] = ccm.nodeValueHistory.temSafeVaule[shardID][nodeID]
			ipPerformanceValue[ip] = ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID]
			iptemPerformanceValue[ip] = ccm.nodeValueHistory.temPerformanceVaule[shardID][nodeID]
		}
	}
	for shardID, nodelist := range newIptable {
		if shardID == params.SupervisorShard {
			continue
		}
		for nodeID, ip := range nodelist {
			if ccm.nodeValueHistory.nodeSafeVaule == nil || ccm.nodeValueHistory.temSafeVaule == nil {
				ccm.nodeValueHistory.nodeSafeVaule = make(map[uint64]map[uint64]float32)
				ccm.nodeValueHistory.nodePerformanceVaule = make(map[uint64]map[uint64]float32)
				ccm.nodeValueHistory.temSafeVaule = make(map[uint64]map[uint64]float32)
				ccm.nodeValueHistory.temPerformanceVaule = make(map[uint64]map[uint64]float32)
			}
			if _, ok := ccm.nodeValueHistory.nodeSafeVaule[shardID]; !ok {
				ccm.nodeValueHistory.nodeSafeVaule[shardID] = make(map[uint64]float32)
				ccm.nodeValueHistory.nodePerformanceVaule[shardID] = make(map[uint64]float32)
			}
			if _, ok := ccm.nodeValueHistory.temSafeVaule[shardID]; !ok {
				ccm.nodeValueHistory.temSafeVaule[shardID] = make(map[uint64]float32)
				ccm.nodeValueHistory.temPerformanceVaule[shardID] = make(map[uint64]float32)
			}
			ccm.nodeValueHistory.nodeSafeVaule[shardID][nodeID] = ipSafeValue[ip]
			ccm.nodeValueHistory.temSafeVaule[shardID][nodeID] = iptemSafeValue[ip]
			ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID] = ipPerformanceValue[ip]
			ccm.nodeValueHistory.temPerformanceVaule[shardID][nodeID] = iptemPerformanceValue[ip]
		}
	}
	//更新分片性能值
	ccm.shardPerformance = ccm.calculateShardPerformance()
}

func (ccm *RLPACommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(ccm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)

	batchId := 0
	epochAfterAccAlloc := 0
	//shardTxNumHistory := make(map[int]map[uint64]uint64)
	ifRLPAed := make(map[int]bool)
	//节点物理Ip-作恶IP的映射
	maliciousIP := make(map[string]string)

	ccm.sl.Slog.Printf("Supervisor: len(plcm.IpNodeTable) is %d\n", len(ccm.IpNodeTable))
	ccm.sl.Slog.Printf("Supervisor: len(plcm.shardLoadHistory.nodeSafeVaule) is %d\n", len(ccm.nodeValueHistory.nodeSafeVaule))

	ccm.sl.Slog.Printf("Supervisor: epoch %d begins, start sending Tx.\n", ccm.epochId)
	ccm.sl.Slog.Printf("Supervisor: ccm.batchDataNum is %d\n", int(ccm.batchDataNum))
	needAccountAlloc := false
	stopepoch := 0
	rlpaCnt := 0
	// flag := true

	// NodeFlag := true
	// clpaCnt := 0
	for {
		if ccm.Ss.EpochEnough() {
			ccm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
			return
		}
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
			if ccm.nodeAllocLastRunTime.IsZero() {
				ccm.nodeAllocLastRunTime = time.Now()
			}
			ccm.txSending(txlist)

			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			ccm.Ss.StopGap_Reset()
			batchId++
		}

		if !ccm.nodeAllocLastRunTime.IsZero() && time.Since(ccm.nodeAllocLastRunTime) >= time.Duration(ccm.nodeAllocFreq)*time.Second {
			// ifchange := plcm.Ss.StopEpoch_update(stopepoch + 1)
			// if ifchange {
			// 	plcm.sl.Slog.Printf("Supervisor: StopEpoch updated to %d", plcm.epochId+1)
			// }
			if ccm.Ss.EpochEnough() {
				ccm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
				return
			}
			if params.IfNodeAlloc {
				//如果本epoch进行了账户分配，那么不进行节点分配了，直接进入下一个epoch
				if ifRLPAed[ccm.epochId] {
					ccm.nodeAllocLastRunTime = time.Now()
					ccm.sl.Slog.Printf("Supervisor: epoch %d finishs, it allocated accounts.\n", ccm.epochId)
					ccm.epochId++
					epochAfterAccAlloc = 0
				} else {
					ccm.sl.Slog.Printf("Supervisor: epoch %d begins to allocate nodes.\n", ccm.epochId)

					//在每次节点划分前更新各节点贡献值、分片性能的操作。
					ccm.updateNodeValue(time.Since(ccm.nodeAllocLastRunTime), ccm.epochId)
					ccm.rlpaGraph.UpdateShardPerformance(ccm.shardPerformance)
					//先判断节点分配，根据当前节点的全局贡献值、分片-节点映射、上个epoch分片的交易负载进行分配。
					//声明一个NodeAllocate对象
					shardLoad := make(map[uint64]float64)
					shardLoadEpochids := make(map[uint64]int)
					for key, val := range ccm.shardLoadHistory {
						shardLoad[key] = val[len(val)-1]
						shardLoadEpochids[key] = len(val) - 1
					}
					//将ccm.IpNodeTable除了最后一个映射外的所有映射进行节点分配
					workIptable := make(map[uint64]map[uint64]string)
					for key, val := range ccm.IpNodeTable {
						if key != params.SupervisorShard {
							workIptable[key] = val
						}
					}
					//输出workIptable映射
					ccm.SaveShardValueToCSV(ccm.epochId, true)
					ccm.sl.Slog.Printf("Supervisor: shardLoadEpochids before nodeAlloc is %v\n", shardLoadEpochids)
					ccm.sl.Slog.Printf("Supervisor: shardLoad before nodeAlloc is %v\n", shardLoad)
					ccm.sl.Slog.Printf("Supervisor: workIptable before nodeAlloc is %v\n", workIptable)
					nodeAlloc := nodeAllocate.NewRLPANodeAllocate(ccm.nodeValueHistory.nodeSafeVaule, ccm.nodeValueHistory.nodePerformanceVaule, ccm.nodeValueHistory.nodePerformanceVauleNorm, ccm.nodeValueHistory.nodeSafeVauleNorm, workIptable, shardLoad, ccm.sl)
					newWorkIptable, iferr := nodeAlloc.RLPANodeAllocation(ccm.epochId)
					if iferr {
						needAccountAlloc = true
						ccm.sl.Slog.Printf("Supervisor: epoch %d allocate nodes err, it begins to allocate accounts.\n", ccm.epochId)
					} else {
						ccm.sl.Slog.Printf("Supervisor: workIptable after nodeAlloc is %v\n", newWorkIptable)
						//更新节点贡献值记录、节点-分片映射、分片性能
						ccm.updateValueAfterMove(newWorkIptable)
						//更新plstate内部的数据
						ccm.rlpaGraph.UpdateData(newWorkIptable, ccm.shardPerformance)
						//更新作恶IP
						maliciousIP = ccm.updateMaliciousIP(ccm.IpNodeTable, newWorkIptable)
						//记录节点分配完的分片值
						ccm.SaveShardValueToCSV(ccm.epochId, false)
						//发送消息广播新的作恶IP
						ccm.nodeAllocResSend(maliciousIP, ccm.epochId+1)

						// plcm.nodeAllocLastRunTime = time.Now()
						ccm.sl.Slog.Printf("Supervisor: epoch %d finishs, it allocated nodes normally.\n", ccm.epochId)
						ccm.epochId++
						epochAfterAccAlloc++
					}
					ccm.nodeAllocLastRunTime = time.Now()
					stopepoch++
				}

			}
		}

		// if params.ShardNum > 1 && !ccm.rlpaLastRunningTime.IsZero() && time.Since(ccm.rlpaLastRunningTime) >= time.Duration(0.5*float64(ccm.rlpaFreq))*time.Second && time.Since(ccm.rlpaLastRunningTime) < time.Duration(ccm.rlpaFreq)*time.Second && flag {
		// 	ccm.rlpaLock.Lock()
		// 	rlpaCnt++
		// 	mmap, _ := ccm.rlpaGraph.RLPA_Partition()

		// 	ccm.rlpaMapSend(mmap)
		// 	for key, val := range mmap {
		// 		ccm.modifiedMap[key] = val
		// 	}
		// 	ccm.rlpaReset()
		// 	ccm.rlpaLock.Unlock()
		// 	flag = false
		// 	ccm.epochId++
		// 	ccm.sl.Slog.Println("Run RLPA Duration ", ccm.epochId)
		// 	if ccm.nowDataNum == ccm.dataTotalNum {
		// 		break
		// 	}
		// }

		if params.ShardNum > 1 && !ccm.rlpaLastRunningTime.IsZero() && time.Since(ccm.rlpaLastRunningTime) >= time.Duration(ccm.rlpaFreq)*time.Second || needAccountAlloc {
			if needAccountAlloc || time.Since(ccm.nodeAllocLastRunTime) >= time.Duration(ccm.nodeAllocFreq-10)*time.Second {
				if ccm.Ss.EpochEnough() {
					ccm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
					return
				}
				ccm.rlpaLock.Lock()
				ccm.sl.Slog.Printf("Supervisor: epoch %d begins to allocate accounts.\n", ccm.epochId)
				rlpaCnt++
				//账户划分前也要更新各节点贡献值、分片性能的操作。
				ccm.updateNodeValue(time.Since(ccm.nodeAllocLastRunTime), ccm.epochId)
				ccm.rlpaGraph.UpdateShardPerformance(ccm.shardPerformance)

				//打印分片的性能值和负载
				ccm.OutputShardPerforAndLoad()
				//打印节点的安全贡献值和性能贡献值
				ccm.OutputNodeValue()

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
				time.Sleep(10 * time.Second)
				// for atomic.LoadInt32(&ccm.curEpoch) != int32(rlpaCnt) {
				// 	time.Sleep(time.Second)
				// }

				ccm.rlpaLastRunningTime = time.Now()
				ifRLPAed[ccm.epochId] = true
				ccm.sl.Slog.Printf("Supervisor: epoch %d allocated accounts successfully.\n", ccm.epochId)
				// flag = true
				if !params.IfNodeAlloc {
					ccm.epochId++
					ccm.nodeAllocLastRunTime = time.Now()
				}
				needAccountAlloc = false
				ccm.sl.Slog.Println("Next RLPA epoch begins. ")
			}
		}

		if ccm.nowDataNum == ccm.dataTotalNum {
			break
		}
	}

	// // all transactions are sent. keep sending partition message...
	// for !ccm.Ss.GapEnough() && !ccm.Ss.EpochEnough() { // wait all txs to be handled
	// 	time.Sleep(time.Second)
	// 	if time.Since(ccm.rlpaLastRunningTime) >= time.Duration(ccm.rlpaFreq)*time.Second {
	// 		ccm.rlpaLock.Lock()
	// 		rlpaCnt++
	// 		mmap, _ := ccm.rlpaGraph.RLPA_Partition()
	// 		ccm.rlpaMapSend(mmap)
	// 		for key, val := range mmap {
	// 			ccm.modifiedMap[key] = val
	// 		}
	// 		ccm.rlpaReset()
	// 		ccm.rlpaLock.Unlock()

	// 		// ccm.clpaLock.Lock()
	// 		// clpaCnt++
	// 		// rlpaCnt++
	// 		// mmap, _ := ccm.clpaGraph.CLPA_Partition()
	// 		// ccm.clpaMapSend(mmap)
	// 		// for key, val := range mmap {
	// 		// 	ccm.modifiedMap[key] = val
	// 		// }
	// 		// ccm.clpaReset()
	// 		// ccm.clpaLock.Unlock()
	// 		for atomic.LoadInt32(&ccm.curEpoch) != int32(rlpaCnt) {
	// 			time.Sleep(time.Second)
	// 		}
	// 		// for atomic.LoadInt32(&ccm.curEpoch) != int32(clpaCnt) {
	// 		// 	time.Sleep(time.Second)
	// 		// }
	// 		ccm.sl.Slog.Printf("Current Epoch: %d, RLPA Count: %d\n", ccm.curEpoch, rlpaCnt)
	// 		ccm.sl.Slog.Println("Next RLPA epoch begins. ")
	// 		ccm.rlpaLastRunningTime = time.Now()
	// 	}
	// }
	// all transactions are sent. keep sending partition message...
	for !ccm.Ss.GapEnough() && !ccm.Ss.EpochEnough() { // wait all txs to be handled
		time.Sleep(time.Second)

		if time.Since(ccm.nodeAllocLastRunTime) >= time.Duration(ccm.nodeAllocFreq)*time.Second {
			if ccm.Ss.EpochEnough() {
				ccm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
				return
			}
			if params.IfNodeAlloc {
				//如果本epoch进行了账户分配，那么不进行节点分配了，直接进入下一个epoch
				if ifRLPAed[ccm.epochId] {
					ccm.nodeAllocLastRunTime = time.Now()
					ccm.sl.Slog.Printf("Supervisor: epoch %d finishs, it allocated accounts.\n", ccm.epochId)
					ccm.epochId++
					epochAfterAccAlloc = 0
				} else {
					ccm.sl.Slog.Printf("Supervisor: epoch %d begins to allocate nodes.\n", ccm.epochId)

					//在每次节点划分前更新各节点贡献值、分片性能的操作。
					ccm.updateNodeValue(time.Since(ccm.nodeAllocLastRunTime), ccm.epochId)
					ccm.rlpaGraph.UpdateShardPerformance(ccm.shardPerformance)
					//先判断节点分配，根据当前节点的全局贡献值、分片-节点映射、上个epoch分片的交易负载进行分配。
					//声明一个NodeAllocate对象
					shardLoad := make(map[uint64]float64)
					shardLoadEpochids := make(map[uint64]int)
					for key, val := range ccm.shardLoadHistory {
						shardLoad[key] = val[len(val)-1]
						shardLoadEpochids[key] = len(val) - 1
					}
					//ccm.IpNodeTable除了最后一个映射外的所有映射进行节点分配
					workIptable := make(map[uint64]map[uint64]string)
					for key, val := range ccm.IpNodeTable {
						if key != params.SupervisorShard {
							workIptable[key] = val
						}
					}
					//输出workIptable映射
					ccm.SaveShardValueToCSV(ccm.epochId, true)
					ccm.sl.Slog.Printf("Supervisor: shardLoadEpochids before nodeAlloc is %v\n", shardLoadEpochids)
					ccm.sl.Slog.Printf("Supervisor: shardLoad before nodeAlloc is %v\n", shardLoad)
					ccm.sl.Slog.Printf("Supervisor: workIptable before nodeAlloc is %v\n", workIptable)
					nodeAlloc := nodeAllocate.NewRLPANodeAllocate(ccm.nodeValueHistory.nodeSafeVaule, ccm.nodeValueHistory.nodePerformanceVaule, ccm.nodeValueHistory.nodePerformanceVauleNorm, ccm.nodeValueHistory.nodeSafeVauleNorm, workIptable, shardLoad, ccm.sl)
					newWorkIptable, iferr := nodeAlloc.RLPANodeAllocation(ccm.epochId)
					if iferr {
						needAccountAlloc = true
						ccm.sl.Slog.Printf("Supervisor: epoch %d allocate nodes err, it begins to allocate accounts.\n", ccm.epochId)
					} else {
						ccm.sl.Slog.Printf("Supervisor: workIptable after nodeAlloc is %v\n", newWorkIptable)
						//更新节点贡献值记录、节点-分片映射、分片性能
						ccm.updateValueAfterMove(newWorkIptable)
						//更新plstate内部的数据
						ccm.rlpaGraph.UpdateData(newWorkIptable, ccm.shardPerformance)
						//更新作恶IP
						maliciousIP = ccm.updateMaliciousIP(ccm.IpNodeTable, newWorkIptable)
						//记录节点分配完的分片值
						ccm.SaveShardValueToCSV(ccm.epochId, false)
						//发送消息广播新的作恶IP
						ccm.nodeAllocResSend(maliciousIP, ccm.epochId+1)

						//plcm.nodeAllocLastRunTime = time.Now()
						ccm.sl.Slog.Printf("Supervisor: epoch %d finishs, it allocated nodes normally.\n", ccm.epochId)
						ccm.epochId++
						epochAfterAccAlloc++
					}
					ccm.nodeAllocLastRunTime = time.Now()
					stopepoch++
				}
			}
		}
		if params.ShardNum > 1 && time.Since(ccm.rlpaLastRunningTime) >= time.Duration(ccm.rlpaFreq)*time.Second || needAccountAlloc {
			if needAccountAlloc || time.Since(ccm.nodeAllocLastRunTime) >= time.Duration(ccm.nodeAllocFreq-10)*time.Second {
				if ccm.Ss.EpochEnough() {
					ccm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
					return
				}
				ccm.rlpaLock.Lock()
				rlpaCnt++
				//账户划分前也要更新各节点贡献值、分片性能的操作。
				ccm.updateNodeValue(time.Since(ccm.nodeAllocLastRunTime), ccm.epochId)
				ccm.rlpaGraph.UpdateShardPerformance(ccm.shardPerformance)

				//打印分片的性能值和负载
				ccm.OutputShardPerforAndLoad()
				//打印节点的安全贡献值和性能贡献值
				ccm.OutputNodeValue()

				mmap, _ := ccm.rlpaGraph.RLPA_Partition()

				ccm.rlpaMapSend(mmap)
				for key, val := range mmap {
					ccm.modifiedMap[key] = val
				}
				ccm.rlpaReset()
				ccm.rlpaLock.Unlock()

				time.Sleep(10 * time.Second)
				// for atomic.LoadInt32(&ccm.curEpoch) != int32(rlpaCnt) {
				// 	time.Sleep(time.Second)
				// }

				ccm.rlpaLastRunningTime = time.Now()
				ifRLPAed[ccm.epochId] = true
				// flag = true
				ccm.sl.Slog.Printf("Supervisor: epoch %d allocated accounts successfully.\n", ccm.epochId)
				if !params.IfNodeAlloc {
					ccm.epochId++
					ccm.nodeAllocLastRunTime = time.Now()
				}
				needAccountAlloc = false
				ccm.sl.Slog.Println("Next RLPA epoch begins. ")
			}
		}
	}

}

// 这里注意oldIptable里包含了最后一个映射即监督节点映射，newIptable里不包含
func (ccm *RLPACommitteeModule) updateMaliciousIP(oldIptable, newIptable map[uint64]map[uint64]string) map[string]string {
	maliciousIP := make(map[string]string)
	for shardID, nodelist := range oldIptable {
		if shardID == params.SupervisorShard {
			continue
		}
		for nodeID, ip := range nodelist {
			if maliIP, ok := newIptable[shardID][nodeID]; ok {
				maliciousIP[ip] = maliIP
			}
		}
	}
	return maliciousIP
}
func (ccm *RLPACommitteeModule) nodeAllocResSend(maliciousIP map[string]string, newEpochId int) {
	// send node alocation result message
	nnm := message.NodeAllocResult{
		NodeMaliciousIP: maliciousIP,
		EpochID:         newEpochId,
	}
	nnmByte, err := json.Marshal(nnm)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.NodeAllocMsg, nnmByte)
	// send to worker shards
	for i := uint64(0); i < uint64(len(ccm.IpNodeTable)-1); i++ {
		for j := uint64(0); j < uint64(len(ccm.IpNodeTable[i])); j++ {
			networks.TcpDial(send_msg, ccm.IpNodeTable[i][j])
		}
	}
	ccm.sl.Slog.Println("Supervisor: node allocation result message has been sent. ")
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

func (ccm *RLPACommitteeModule) rlpaReset() {
	ccm.rlpaGraph = new(partition.RLPAState)
	ccm.rlpaGraph.Init_RLPAState(params.Beta, ccm.shardPerformance, 0.5, 100, params.ShardNum)
	for key, val := range ccm.modifiedMap {
		ccm.rlpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
}

func (ccm *RLPACommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	ccm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	// if atomic.CompareAndSwapInt32(&ccm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
	// 	ccm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	// }
	if b.BlockBodyLength == 0 {
		return
	}
	if b.Epoch != ccm.epochId {
		ccm.sl.Slog.Printf("Supervisor: received BlockInfo epoch is not equal to epochID in Supervisor! \n")
		ccm.sl.Slog.Println("epochId:", ccm.epochId, "received epoch:", b.Epoch)
		return
	}
	ccm.rlpaLock.Lock()
	loadChange := float64(len(b.InnerShardTxs)) + float64(params.Beta)*float64(uint64(len(b.Relay2Txs)))
	if _, ok := ccm.shardLoadHistory[b.SenderShardID]; !ok {
		ccm.shardLoadHistory[b.SenderShardID] = make([]float64, 0)
	}
	if len(ccm.shardLoadHistory[b.SenderShardID]) == b.Epoch {
		ccm.shardLoadHistory[b.SenderShardID] = append(ccm.shardLoadHistory[b.SenderShardID], loadChange)
	} else if len(ccm.shardLoadHistory[b.SenderShardID]) == b.Epoch+1 {
		ccm.shardLoadHistory[b.SenderShardID][b.Epoch] += loadChange
	} else {
		ccm.sl.Slog.Printf("Supervisor: shard %d load history length is wrong.\n", b.SenderShardID)
	}
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
func (ccm *RLPACommitteeModule) OutputNodeValue() {
	ccm.sl.Slog.Printf("Supervisor: node safe value is following ... \n")
	for i := 0; i < len(ccm.nodeValueHistory.nodeSafeVaule); i++ {
		ccm.sl.Slog.Printf("Shard %d :", i)
		//声明临时数组将节点安全贡献值存储，然后输出数组
		//tem := make([]float32, len(plcm.nodeValueHistory.nodeSafeVaule[uint64(i)]))
		var tem []interface{}
		for j := 0; j < len(ccm.nodeValueHistory.nodeSafeVaule[uint64(i)]); j++ {
			tem = append(tem, ccm.nodeValueHistory.nodeSafeVaule[uint64(i)][uint64(j)])
		}
		ccm.sl.Slog.Printf("%v\n", tem)
	}
	ccm.sl.Slog.Printf("Supervisor: node safe value norm is following ... \n")
	for i := 0; i < len(ccm.nodeValueHistory.nodeSafeVaule); i++ {
		ccm.sl.Slog.Printf("Shard %d :", i)
		//声明临时数组将节点安全贡献值存储，然后输出数组
		//tem := make([]float32, len(plcm.nodeValueHistory.nodeSafeVaule[uint64(i)]))
		var tem []interface{}
		for j := 0; j < len(ccm.nodeValueHistory.nodeSafeVauleNorm[uint64(i)]); j++ {
			tem = append(tem, ccm.nodeValueHistory.nodeSafeVauleNorm[uint64(i)][uint64(j)])
		}
		ccm.sl.Slog.Printf("%v\n", tem)
	}
	ccm.sl.Slog.Printf("Supervisor: node performance value is  following ... \n")
	for i := 0; i < len(ccm.nodeValueHistory.nodePerformanceVaule); i++ {
		ccm.sl.Slog.Printf("Shard %d :", i)
		//tem := make([]float32, len(plcm.nodeValueHistory.nodePerformanceVaule[uint64(i)]))
		var tem []interface{}
		for j := 0; j < len(ccm.nodeValueHistory.nodePerformanceVaule[uint64(i)]); j++ {
			tem = append(tem, ccm.nodeValueHistory.nodePerformanceVaule[uint64(i)][uint64(j)])
		}
		ccm.sl.Slog.Printf("%v\n", tem)
	}
	ccm.sl.Slog.Printf("Supervisor: node performance value norm is  following ... \n")
	for i := 0; i < len(ccm.nodeValueHistory.nodePerformanceVaule); i++ {
		ccm.sl.Slog.Printf("Shard %d :", i)
		//tem := make([]float32, len(plcm.nodeValueHistory.nodePerformanceVaule[uint64(i)]))
		var tem []interface{}
		for j := 0; j < len(ccm.nodeValueHistory.nodePerformanceVauleNorm[uint64(i)]); j++ {
			tem = append(tem, ccm.nodeValueHistory.nodePerformanceVauleNorm[uint64(i)][uint64(j)])
		}
		ccm.sl.Slog.Printf("%v\n", tem)
	}
	ccm.sl.Slog.Printf("Supervisor: ShardLoad is  following ... \n")
	for shardID, loadHistory := range ccm.shardLoadHistory {
		ccm.sl.Slog.Printf("Shard %d :", shardID)
		// tem := make([]float64, len(loadHistory))
		var tem []interface{}
		for j := 0; j < len(loadHistory); j++ {
			tem = append(tem, loadHistory[j])
		}
		ccm.sl.Slog.Printf("%v\n", tem)
	}
	ccm.sl.Slog.Printf("Supervisor: node value is  following ... \n")
	for i := 0; i < len(ccm.nodeValueHistory.nodePerformanceVaule); i++ {
		ccm.sl.Slog.Printf("Shard %d :", i)
		//tem := make([]float32, len(plcm.nodeValueHistory.nodePerformanceVaule[uint64(i)]))
		var tem []interface{}
		for j := 0; j < len(ccm.nodeValueHistory.nodePerformanceVauleNorm[uint64(i)]); j++ {
			tem = append(tem, ccm.nodeValueHistory.nodePerformanceVauleNorm[uint64(i)][uint64(j)]*0.6+ccm.nodeValueHistory.nodeSafeVauleNorm[uint64(i)][uint64(j)]*0.4)
		}
		ccm.sl.Slog.Printf("%v\n", tem)
	}
}

// 打印分片性能值和最新负载
func (ccm *RLPACommitteeModule) OutputShardPerforAndLoad() {
	ccm.sl.Slog.Printf("Supervisor: shard performance is following ...\n")
	ccm.sl.Slog.Printf("%v\n", ccm.shardPerformance)

	epochid := len(ccm.shardLoadHistory[uint64(0)]) - 1
	ccm.sl.Slog.Printf("Supervisor: shard tx num in latest epoch %d is following ...\n", epochid)
	// temLoad := make([]uint64, len(shardTxNumHistory[epochId]))
	var temLoad []interface{}
	var epochids []interface{}
	for i := 0; i < len(ccm.shardLoadHistory); i++ {
		temLoad = append(temLoad, ccm.shardLoadHistory[uint64(i)][len(ccm.shardLoadHistory[uint64(i)])-1])
		epochids = append(epochids, len(ccm.shardLoadHistory[uint64(i)])-1)
	}
	ccm.sl.Slog.Printf("epochids:%v\n", epochids)
	ccm.sl.Slog.Printf("temLoad:%v\n", temLoad)
}

// 计算分片内节点安全值的平均值作为分片的安全值，计算分片安全值的方差和平均值写入文件
func (ccm *RLPACommitteeModule) SaveShardValueToCSV(epochId int, ifBegin bool) {
	//计算分片安全值
	ccm.sl.Slog.Println("Supervisor: calculate shard safe value...")
	shardSafeValue := make(map[uint64]float32)
	for shardID, nodeAction := range ccm.nodeValueHistory.nodeSafeVaule {
		sum := float32(0)
		for _, safeValue := range nodeAction {
			sum += safeValue
		}
		average := sum / float32(len(nodeAction))
		shardSafeValue[shardID] = average
	}
	//计算分片安全值的平均值和方差
	sum := float32(0)
	for _, safeValue := range shardSafeValue {
		sum += safeValue
	}
	average := sum / float32(len(shardSafeValue))
	variance := float32(0)
	for _, safeValue := range shardSafeValue {
		variance += (safeValue - average) * (safeValue - average)
	}
	variance = variance / float32(len(shardSafeValue))

	//将分片安全值的平均值和方差以续写的方式写入文件,第一列是epochID,第二列是begin/end,第三列是平均值，第四列是方差
	file, err := os.OpenFile(params.DataWrite_path+"shardSafeValue.csv", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		ccm.sl.Slog.Printf("Supervisor:%v\n", err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	//如果文件是空的，写入表头
	if fileinfo, err := file.Stat(); err == nil && fileinfo.Size() == 0 {
		writer.Write([]string{"epochID", "nodeAlloc's begin/end", "shardSafeValueAverage", "shardSafeValueVariance"})
	}
	if ifBegin {
		writer.Write([]string{strconv.Itoa(epochId), "begin", strconv.FormatFloat(float64(average), 'f', 8, 64), strconv.FormatFloat(float64(variance), 'f', 8, 64)})
	} else {
		writer.Write([]string{strconv.Itoa(epochId), "end", strconv.FormatFloat(float64(average), 'f', 8, 64), strconv.FormatFloat(float64(variance), 'f', 8, 64)})
	}
}

func (ccm *RLPACommitteeModule) MigrateNode(nodeID, sourceShardID, targetShardID uint64) error {
	// 检查节点是否存在于源分片
	if _, ok := ccm.IpNodeTable[sourceShardID][nodeID]; !ok {
		return fmt.Errorf("node %d does not exist in shard %d", nodeID, sourceShardID)
	}

	// 检查目标分片是否有效
	if _, ok := ccm.IpNodeTable[targetShardID]; !ok {
		ccm.IpNodeTable[targetShardID] = make(map[uint64]string)
	}

	// 获取节点的 IP 地址
	nodeIP := ccm.IpNodeTable[sourceShardID][nodeID]

	// 更新分片信息
	delete(ccm.IpNodeTable[sourceShardID], nodeID)
	ccm.IpNodeTable[targetShardID][nodeID] = nodeIP

	// 通知节点更新其分片信息
	migrateMsg := message.NodeMigrationMsg{
		NodeID:   nodeID,
		NewIP:    nodeIP,
		NewShard: targetShardID,
	}

	migrateMsgBytes, _ := json.Marshal(migrateMsg)
	for _, nodeMap := range ccm.IpNodeTable {
		for _, ip := range nodeMap {
			go networks.TcpDial(message.MergeMessage(message.CNodeMigration, migrateMsgBytes), ip)
		}
	}

	// 日志记录
	ccm.sl.Slog.Printf("Migrated node %d from shard %d to shard %d\n", nodeID, sourceShardID, targetShardID)

	return nil
}

func (ccm *RLPACommitteeModule) IterativeMigration(group1, group2 []uint64, iterations int) error {
	totalNodes := 0
	for _, shard := range ccm.IpNodeTable {
		totalNodes += len(shard)
	}
	avgNodes := totalNodes / len(ccm.IpNodeTable) // 平均节点数
	numShards := len(ccm.IpNodeTable)             // 分片总数

	// 随机选择起始组
	currentGroup := group1
	isGroup1 := true
	if rand.Intn(2) == 1 {
		currentGroup = group2
		isGroup1 = false
	}

	// 随机选择当前组中的一个分片
	currentShardID := currentGroup[rand.Intn(len(currentGroup))]

	// 从当前分片中随机选择一个节点（排除节点 0）
	var selectedNode uint64
	for nodeID := range ccm.IpNodeTable[currentShardID] {
		if nodeID != 0 {
			selectedNode = nodeID
			break
		}
	}
	if selectedNode == 0 {
		ccm.sl.Slog.Printf("No valid node found in shard %d\n", currentShardID)
		return nil
	}

	// 随机选择目标组中的一个分片
	targetGroup := group1
	if isGroup1 {
		targetGroup = group2
	}
	targetShardID := targetGroup[rand.Intn(len(targetGroup))]

	// 将选中的节点迁移到目标分片
	err := ccm.MigrateNode(selectedNode, currentShardID, targetShardID)
	if err != nil {
		ccm.sl.Slog.Printf("Failed to migrate node %d from shard %d to shard %d: %v\n", selectedNode, currentShardID, targetShardID, err)
		return err
	}

	// 开始递归迁移
	return ccm.recursiveMigration(targetShardID, group1, group2, iterations, avgNodes, numShards)
}

func (ccm *RLPACommitteeModule) recursiveMigration(currentShardID uint64, group1, group2 []uint64, iterations int, avgNodes, numShards int) error {
	if iterations <= 0 {
		return nil // 终止条件
	}

	currentShardNodes := len(ccm.IpNodeTable[currentShardID])
	nodesToMigrate := (numShards * currentShardNodes) / avgNodes
	if nodesToMigrate == 0 {
		return nil
	}

	// 获取可迁移节点
	var nodesToMove []uint64
	for nodeID := range ccm.IpNodeTable[currentShardID] {
		if nodeID != 0 {
			nodesToMove = append(nodesToMove, nodeID)
		}
	}
	rand.Shuffle(len(nodesToMove), func(i, j int) {
		nodesToMove[i], nodesToMove[j] = nodesToMove[j], nodesToMove[i]
	})
	if len(nodesToMove) > nodesToMigrate {
		nodesToMove = nodesToMove[:nodesToMigrate]
	}

	// 随机选择目标组
	targetGroup := group1
	if containsGroup(group1, currentShardID) {
		targetGroup = group2
	}

	// 依次将节点迁移到目标组的分片，并递归
	for _, nodeID := range nodesToMove {
		if _, ok := ccm.IpNodeTable[currentShardID][nodeID]; !ok {
			continue
		}
		targetShardID := targetGroup[rand.Intn(len(targetGroup))]
		err := ccm.MigrateNode(nodeID, currentShardID, targetShardID)
		if err != nil {
			ccm.sl.Slog.Printf("Failed to migrate node %d from shard %d to shard %d: %v\n", nodeID, currentShardID, targetShardID, err)
			continue
		}
		// 递归只对本次迁入的分片
		return ccm.recursiveMigration(targetShardID, group1, group2, iterations-1, avgNodes, numShards)
	}
	return nil
}

// 辅助函数：判断分片是否属于某个组
func containsGroup(group []uint64, shardID uint64) bool {
	for _, id := range group {
		if id == shardID {
			return true
		}
	}
	return false
}

func (ccm *RLPACommitteeModule) PrintShardNodes() {
	for shardID, nodeMap := range ccm.IpNodeTable {
		nodeList := make([]uint64, 0, len(nodeMap))
		for nodeID := range nodeMap {
			nodeList = append(nodeList, nodeID)
		}
		ccm.sl.Slog.Printf("Shard %d nodes: %v\n", shardID, nodeList)
	}
}

// 归一化函数
func (ccm *RLPACommitteeModule) NormalizeNodePerformance() {
	// 生成归一化副本，不覆盖原始数据
	norm := make(map[uint64]map[uint64]float32)
	minVal := float32(1e9)
	maxVal := float32(-1e9)
	for _, nodeMap := range ccm.nodeValueHistory.nodePerformanceVaule {
		for _, v := range nodeMap {
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
	}
	for shardID, nodeMap := range ccm.nodeValueHistory.nodePerformanceVaule {
		norm[shardID] = make(map[uint64]float32)
		for nodeID, v := range nodeMap {
			if maxVal != minVal {
				norm[shardID][nodeID] = (v - minVal) / (maxVal - minVal)
			} else {
				norm[shardID][nodeID] = 1.0
			}
		}
	}
	ccm.nodeValueHistory.nodePerformanceVauleNorm = norm
}

// 归一化函数
func (ccm *RLPACommitteeModule) NormalizeNodeSafeValue() {
	// 生成归一化副本，不覆盖原始数据
	norm := make(map[uint64]map[uint64]float32)
	minVal := float32(1e9)
	maxVal := float32(-1e9)
	for _, nodeMap := range ccm.nodeValueHistory.nodeSafeVaule {
		for _, v := range nodeMap {
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
	}
	for shardID, nodeMap := range ccm.nodeValueHistory.nodeSafeVaule {
		norm[shardID] = make(map[uint64]float32)
		for nodeID, v := range nodeMap {
			if maxVal != minVal {
				norm[shardID][nodeID] = (v - minVal) / (maxVal - minVal)
			} else {
				norm[shardID][nodeID] = 1.0
			}
		}
	}
	ccm.nodeValueHistory.nodeSafeVauleNorm = norm
}
