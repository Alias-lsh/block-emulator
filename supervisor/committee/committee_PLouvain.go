package committee

import (
	"blockEmulator/core"
	"blockEmulator/louvain"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/nodeAlloca"
	"blockEmulator/params"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

// PLouvain committee operations
type PLouvainCommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	// additional variants
	plouvainLock            sync.Mutex
	plouvainState           *louvain.PLouvainState
	modifiedMap             map[string]int //账户地址->分片id
	plouvainLastRunningTime time.Time
	plouvainFreq            int
	plouvainFreaEpoch       int
	shardPerformance        []float32

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal           // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string //分片Id->节点Id->节点IP

	nodeValueHistory     *NodeValueHistory
	nodeAllocLastRunTime time.Time
	nodeAllocFreq        int
	shardLoadHistory     map[uint64][]float64

	epochId int
}

// type ShardLoad struct {
// 	interTx float64
// 	relayTx float64
// 	allTx   float64
// }

// 此处需要传shardPerformance参数
func NewPLouvainCommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog,
	csvFilePath string, dataNum, batchNum int, shardPerformance []float32) *PLouvainCommitteeModule {
	plState := new(louvain.PLouvainState)
	//参数分别为beta, shardNum, shardPerformance, secStageMoveNode, isDeterministic, isTxAllo, israndom，全false是模式一非确定性
	//plState.Init_PLouvainState(params.Beta, params.ShardNum, shardPerformance, false, false, false, false)
	//参数分别为beta, shardNum, shardPerformance, secStageMoveNode, isDeterministic, isTxAllo, israndom，全false是模式一非确定性
	plState.Init_PLouvainState(params.Beta, params.ShardNum, shardPerformance, false, true, false, false)

	newnodeValueHistory := new(NodeValueHistory)
	newnodeValueHistory.Init_NodeValueHistory(Ip_nodeTable)
	//输出nodeSafeVaule的length
	//sl.Slog.Printf("Supervisor: newnodeValueHistory.nodeSafeVaule length is %d\n", len(newnodeValueHistory.nodeSafeVaule))
	shardLoad := make(map[uint64][]float64)
	return &PLouvainCommitteeModule{
		csvPath:                 csvFilePath,
		dataTotalNum:            dataNum,
		batchDataNum:            batchNum,
		nowDataNum:              0,
		plouvainState:           plState,
		modifiedMap:             make(map[string]int),
		plouvainFreq:            params.PlouvainFrequency,
		plouvainFreaEpoch:       params.PlouvainFreqEpoch,
		shardPerformance:        shardPerformance,
		plouvainLastRunningTime: time.Time{},
		IpNodeTable:             Ip_nodeTable,
		Ss:                      Ss,
		sl:                      sl,
		nodeValueHistory:        newnodeValueHistory,
		nodeAllocLastRunTime:    time.Time{},
		nodeAllocFreq:           params.NodeAllocFreq,
		shardLoadHistory:        shardLoad,
		epochId:                 0,
	}
}

func (plcm *PLouvainCommitteeModule) HandleOtherMessage([]byte) {}

func (plcm *PLouvainCommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := plcm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return uint64(val)
	}
}

func (plcm *PLouvainCommitteeModule) txSending(txlist []*core.Transaction) map[uint64]uint64 {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)
	sendTxNum := make(map[uint64]uint64)

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
				//plcm.sl.Slog.Printf("txSending():ready sending %d txs to [%d][0]\n", len(it.Txs), sid)
				go networks.TcpDial(send_msg, plcm.IpNodeTable[sid][0])
				//plcm.sl.Slog.Printf("txSending(): Supervisor: sended %d txs to [%d][0]\n", len(it.Txs), sid)
				if _, ok := sendTxNum[sid]; !ok {
					sendTxNum[sid] = 0
				}
				sendTxNum[sid] += uint64(len(sendToShard[sid]))
			}

			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		sendersid := plcm.fetchModifiedMap(tx.Sender)
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
	return sendTxNum
}

// 监督者处理节点动作消息，更新epoch内节点贡献值变化、贡献交易变化
func (plcm *PLouvainCommitteeModule) HandleNodeAction(content []byte) {
	na := new(message.NodeAction)
	na.SafeVauleInEpoch = make(map[uint64]float32)
	na.TxinEpoch = make(map[uint64]float32)

	err := json.Unmarshal(content, na)
	if err != nil {
		plcm.sl.Slog.Printf("Supervisor: json.Unmarshal error: %v\n", err)
		log.Panic(err)
	}
	plcm.sl.Slog.Printf("Supervisor: begins update node value using nodeAction message.\n")
	//epoch节点贡献值更新
	//打印节点贡献值
	for nodeID, safeValue := range na.SafeVauleInEpoch {
		plcm.sl.Slog.Printf("Supervisor: shard %d node %d safe value is %f\n", na.ShardIndex, nodeID, safeValue)
		plcm.nodeValueHistory.temSafeVaule[na.ShardIndex][nodeID] = safeValue
	}
	for nodeID, txValue := range na.TxinEpoch {
		plcm.sl.Slog.Printf("Supervisor: shard %d node %d tx value is %f\n", na.ShardIndex, nodeID, txValue)
		plcm.nodeValueHistory.temPerformanceVaule[na.ShardIndex][nodeID] = txValue
	}
	plcm.sl.Slog.Printf("Supervisor: have updated node value using nodeAction message.\n")
}

func (plcm *PLouvainCommitteeModule) updateNodeValue(timeDurtionInEpoch time.Duration, epochId int) {
	plcm.sl.Slog.Printf("Supervisor: epoch %d update NodeValue...", epochId)
	duration := timeDurtionInEpoch * time.Nanosecond
	// 将time.Duration转换为秒，并转换为uint64
	timeInEpoch := uint64(duration.Nanoseconds()) / uint64(time.Second)
	if epochId == 0 {
		plcm.nodeValueHistory.nodeSafeVaule = plcm.nodeValueHistory.temSafeVaule
		for shardID, nodeAction := range plcm.nodeValueHistory.temPerformanceVaule {
			for nodeID, txValue := range nodeAction {
				initialValue := txValue / float32(timeInEpoch)
				plcm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID] = initialValue
				plcm.nodeValueHistory.temPerformanceVaule[shardID][nodeID] = plcm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID]
			}
		}
	} else {
		// 根据本epoch贡献值改变量更新节点全局的安全贡献值
		for shardID, nodeAction := range plcm.nodeValueHistory.temSafeVaule {
			for nodeID, safeValue := range nodeAction {
				oldVaule := plcm.nodeValueHistory.nodeSafeVaule[shardID][nodeID]
				plcm.nodeValueHistory.nodeSafeVaule[shardID][nodeID] = params.Alpha*oldVaule + (1-params.Alpha)*safeValue
				plcm.nodeValueHistory.temSafeVaule[shardID][nodeID] = plcm.nodeValueHistory.nodeSafeVaule[shardID][nodeID]
			}
		}
		// 更新节点全局的性能贡献值
		for shardID, nodeAction := range plcm.nodeValueHistory.temPerformanceVaule {
			for nodeID, txValue := range nodeAction {
				oldVaule := plcm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID]
				changeVaule := txValue / float32(timeInEpoch)
				plcm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID] = params.Alpha*oldVaule + (1-params.Alpha)*changeVaule
				plcm.nodeValueHistory.temPerformanceVaule[shardID][nodeID] = plcm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID]
			}
		}
	}
	//更新分片性能值
	plcm.shardPerformance = plcm.calculateShardPerformance()
}

// 根据节点安全、性能贡献值计算分片性能值
func (plcm *PLouvainCommitteeModule) calculateShardPerformance() []float32 {
	shardPerformance := make([]float32, len(plcm.nodeValueHistory.nodePerformanceVaule))
	for i := 0; i < len(plcm.nodeValueHistory.nodePerformanceVaule); i++ {
		shardPerformance[i] = 0
		for j := 0; j < len(plcm.nodeValueHistory.nodePerformanceVaule[uint64(i)]); j++ {
			shardPerformance[i] += plcm.nodeValueHistory.nodePerformanceVaule[uint64(i)][uint64(j)]
		}
	}
	return shardPerformance
}

func (plcm *PLouvainCommitteeModule) updateValueAfterMove(newIptable map[uint64]map[uint64]string) {
	//更新节点贡献值记录
	ipSafeValue := make(map[string]float32)
	iptemSafeValue := make(map[string]float32)
	ipPerformanceValue := make(map[string]float32)
	iptemPerformanceValue := make(map[string]float32)
	for shardID, nodelist := range plcm.IpNodeTable {
		if shardID == params.SupervisorShard {
			continue
		}
		for nodeID, ip := range nodelist {
			ipSafeValue[ip] = plcm.nodeValueHistory.nodeSafeVaule[shardID][nodeID]
			iptemSafeValue[ip] = plcm.nodeValueHistory.temSafeVaule[shardID][nodeID]
			ipPerformanceValue[ip] = plcm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID]
			iptemPerformanceValue[ip] = plcm.nodeValueHistory.temPerformanceVaule[shardID][nodeID]
		}
	}
	for shardID, nodelist := range newIptable {
		if shardID == params.SupervisorShard {
			continue
		}
		for nodeID, ip := range nodelist {
			if plcm.nodeValueHistory.nodeSafeVaule == nil || plcm.nodeValueHistory.temSafeVaule == nil {
				plcm.nodeValueHistory.nodeSafeVaule = make(map[uint64]map[uint64]float32)
				plcm.nodeValueHistory.nodePerformanceVaule = make(map[uint64]map[uint64]float32)
				plcm.nodeValueHistory.temSafeVaule = make(map[uint64]map[uint64]float32)
				plcm.nodeValueHistory.temPerformanceVaule = make(map[uint64]map[uint64]float32)
			}
			if _, ok := plcm.nodeValueHistory.nodeSafeVaule[shardID]; !ok {
				plcm.nodeValueHistory.nodeSafeVaule[shardID] = make(map[uint64]float32)
				plcm.nodeValueHistory.nodePerformanceVaule[shardID] = make(map[uint64]float32)
			}
			if _, ok := plcm.nodeValueHistory.temSafeVaule[shardID]; !ok {
				plcm.nodeValueHistory.temSafeVaule[shardID] = make(map[uint64]float32)
				plcm.nodeValueHistory.temPerformanceVaule[shardID] = make(map[uint64]float32)
			}
			plcm.nodeValueHistory.nodeSafeVaule[shardID][nodeID] = ipSafeValue[ip]
			plcm.nodeValueHistory.temSafeVaule[shardID][nodeID] = iptemSafeValue[ip]
			plcm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID] = ipPerformanceValue[ip]
			plcm.nodeValueHistory.temPerformanceVaule[shardID][nodeID] = iptemPerformanceValue[ip]
		}
	}
	//更新分片性能值
	plcm.shardPerformance = plcm.calculateShardPerformance()
}

func (plcm *PLouvainCommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(plcm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)

	batchId := 0
	epochAfterAccAlloc := 0
	//shardTxNumHistory := make(map[int]map[uint64]uint64)
	ifPlouvained := make(map[int]bool)
	//节点物理Ip-作恶IP的映射
	maliciousIP := make(map[string]string)

	plcm.sl.Slog.Printf("Supervisor: len(plcm.IpNodeTable) is %d\n", len(plcm.IpNodeTable))
	plcm.sl.Slog.Printf("Supervisor: len(plcm.shardLoadHistory.nodeSafeVaule) is %d\n", len(plcm.nodeValueHistory.nodeSafeVaule))

	plcm.sl.Slog.Printf("Supervisor: epoch %d begins, start sending Tx.\n", plcm.epochId)
	plcm.sl.Slog.Printf("Supervisor: plcm.batchDataNum is %d\n", int(plcm.batchDataNum))
	needAccountAlloc := false
	stopepoch := 0
	for {
		if plcm.Ss.EpochEnough() {
			plcm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
			return
		}
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		if tx, ok := data2tx(data, uint64(plcm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			plcm.nowDataNum++
		} else {
			continue
		}

		// if len(txlist)%1000 == 0 {
		// 	plcm.sl.Slog.Printf("Supervisor: epoch %d, txlist length is %d. \n", plcm.epochId, len(txlist))
		// }
		// batch sending condition
		if len(txlist) == int(plcm.batchDataNum) || plcm.nowDataNum == plcm.dataTotalNum {
			//日志打印表示攒够一批交易
			//plcm.sl.Slog.Printf("Supervisor: epoch %d start sending [%d]th batch Tx.\n", plcm.epochId, batchId)
			//plcm.sl.Slog.Printf("--Supervisor: txlist length is %d. \n", len(txlist))
			// set the algorithm timer begins
			if plcm.plouvainLastRunningTime.IsZero() {
				plcm.plouvainLastRunningTime = time.Now()
			}
			if plcm.nodeAllocLastRunTime.IsZero() {
				plcm.nodeAllocLastRunTime = time.Now()
			}
			//日志中打印txlist长度
			plcm.sl.Slog.Printf("Supervisor: txlist length is %d. \n", len(txlist))
			plcm.txSending(txlist)
			//shardTxNumHistory[epochId] = sendTxNum

			//打印sendTxNum
			//plcm.sl.Slog.Printf("Supervisor: this batch sendTxNum is following ... \n")
			//for key, val := range sendTxNum {
			//plcm.sl.Slog.Printf("Shard %d : %d\n", key, val)
			//}

			// reset the variants about tx sending 这里会清空
			txlist = make([]*core.Transaction, 0)
			plcm.Ss.StopGap_Reset()
			batchId++
		}

		if !plcm.nodeAllocLastRunTime.IsZero() && time.Since(plcm.nodeAllocLastRunTime) >= time.Duration(plcm.nodeAllocFreq)*time.Second {
			// ifchange := plcm.Ss.StopEpoch_update(stopepoch + 1)
			// if ifchange {
			// 	plcm.sl.Slog.Printf("Supervisor: StopEpoch updated to %d", plcm.epochId+1)
			// }
			if plcm.Ss.EpochEnough() {
				plcm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
				return
			}
			if params.IfNodeAlloc {
				//如果本epoch进行了账户分配，那么不进行节点分配了，直接进入下一个epoch
				if ifPlouvained[plcm.epochId] {
					plcm.nodeAllocLastRunTime = time.Now()
					plcm.sl.Slog.Printf("Supervisor: epoch %d finishs, it allocated accounts.\n", plcm.epochId)
					plcm.epochId++
					epochAfterAccAlloc = 0
				} else {
					plcm.sl.Slog.Printf("Supervisor: epoch %d begins to allocate nodes.\n", plcm.epochId)

					//在每次节点划分前更新各节点贡献值、分片性能的操作。
					plcm.updateNodeValue(time.Since(plcm.nodeAllocLastRunTime), plcm.epochId)
					plcm.plouvainState.UpdateShardPerformance(plcm.shardPerformance)
					//先判断节点分配，根据当前节点的全局贡献值、分片-节点映射、上个epoch分片的交易负载进行分配。
					//声明一个NodeAllocate对象
					shardLoad := make(map[uint64]float64)
					shardLoadEpochids := make(map[uint64]int)
					for key, val := range plcm.shardLoadHistory {
						shardLoad[key] = val[len(val)-1]
						shardLoadEpochids[key] = len(val) - 1
					}
					//将plcm.IpNodeTable除了最后一个映射外的所有映射进行节点分配
					workIptable := make(map[uint64]map[uint64]string)
					for key, val := range plcm.IpNodeTable {
						if key != params.SupervisorShard {
							workIptable[key] = val
						}
					}
					//输出workIptable映射
					plcm.SaveShardValueToCSV(plcm.epochId, true)
					plcm.sl.Slog.Printf("Supervisor: shardLoadEpochids before nodeAlloc is %v\n", shardLoadEpochids)
					plcm.sl.Slog.Printf("Supervisor: shardLoad before nodeAlloc is %v\n", shardLoad)
					plcm.sl.Slog.Printf("Supervisor: workIptable before nodeAlloc is %v\n", workIptable)
					nodeAlloc := nodeAlloca.NewNodeAllocate(plcm.nodeValueHistory.nodeSafeVaule, plcm.nodeValueHistory.nodePerformanceVaule, workIptable, shardLoad, plcm.sl)
					newWorkIptable, iferr := nodeAlloc.NodeAllocation(plcm.epochId)
					if iferr {
						needAccountAlloc = true
						plcm.sl.Slog.Printf("Supervisor: epoch %d allocate nodes err, it begins to allocate accounts.\n", plcm.epochId)
					} else {
						plcm.sl.Slog.Printf("Supervisor: workIptable after nodeAlloc is %v\n", newWorkIptable)
						//更新节点贡献值记录、节点-分片映射、分片性能
						plcm.updateValueAfterMove(newWorkIptable)
						//更新plstate内部的数据
						plcm.plouvainState.UpdateData(newWorkIptable, plcm.shardPerformance)
						//更新作恶IP
						maliciousIP = plcm.updateMaliciousIP(plcm.IpNodeTable, newWorkIptable)
						//记录节点分配完的分片值
						plcm.SaveShardValueToCSV(plcm.epochId, false)
						//发送消息广播新的作恶IP
						plcm.nodeAllocResSend(maliciousIP, plcm.epochId+1)

						// plcm.nodeAllocLastRunTime = time.Now()
						plcm.sl.Slog.Printf("Supervisor: epoch %d finishs, it allocated nodes normally.\n", plcm.epochId)
						plcm.epochId++
						epochAfterAccAlloc++
					}
					plcm.nodeAllocLastRunTime = time.Now()
					stopepoch++
				}

			}
			//plcm.nodeAllocLastRunTime = time.Now()
			//stopepoch++
		}

		//再判断账户划分,每plouvainFreqEpoch个epoch或者达到plouvainFreq时间会进行一次账户分配，且在epoch的第nodeAllocFreq-10秒（80-10s）进行
		//if needAccountAlloc || !plcm.plouvainLastRunningTime.IsZero() && (time.Since(plcm.plouvainLastRunningTime) >= time.Duration(plcm.plouvainFreq)*time.Second || (epochAfterAccAlloc+1)%plcm.plouvainFreaEpoch == 0) {
		if needAccountAlloc || !plcm.plouvainLastRunningTime.IsZero() && time.Since(plcm.plouvainLastRunningTime) >= time.Duration(plcm.plouvainFreq)*time.Second {
			if needAccountAlloc || time.Since(plcm.nodeAllocLastRunTime) >= time.Duration(plcm.nodeAllocFreq-10)*time.Second {
				plcm.sl.Slog.Printf("Supervisor: 我进来账户划分了.\n")
				// if !params.IfNodeAlloc {
				// 	ifchange := plcm.Ss.StopEpoch_update(plcm.epochId + 1)
				// 	if ifchange {
				// 		plcm.sl.Slog.Printf("Supervisor: StopEpoch updated to %d", plcm.epochId+1)
				// 	}
				if plcm.Ss.EpochEnough() {
					plcm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
					return
				}
				//}
				plcm.plouvainLock.Lock()
				plcm.sl.Slog.Printf("Supervisor: epoch %d begins to allocate accounts.\n", plcm.epochId)

				//账户划分前也要更新各节点贡献值、分片性能的操作。
				plcm.updateNodeValue(time.Since(plcm.nodeAllocLastRunTime), plcm.epochId)
				plcm.plouvainState.UpdateShardPerformance(plcm.shardPerformance)

				//打印分片的性能值和负载
				plcm.OutputShardPerforAndLoad()
				//打印节点的安全贡献值和性能贡献值
				plcm.OutputNodeValue()

				mmap := plcm.plouvainState.PLouvain_Partition()
				//plcm.sl.Slog.Printf("Supervisor: epoch %d allocated accounts mmap:%v \n", plcm.epochId, mmap)
				plcm.plouvainMapSend(mmap)
				for key, val := range mmap {
					plcm.modifiedMap[key] = int(val)
				}
				plcm.plouvainReset()
				plcm.plouvainLock.Unlock()
				time.Sleep(10 * time.Second)
				plcm.plouvainLastRunningTime = time.Now()
				//plcm.nodeAllocLastRunTime = time.Now() //账户分配后更新节点分配时间

				ifPlouvained[plcm.epochId] = true
				plcm.sl.Slog.Printf("Supervisor: epoch %d allocated accounts successfully.\n", plcm.epochId)

				if !params.IfNodeAlloc {
					plcm.epochId++
					plcm.nodeAllocLastRunTime = time.Now()
				}
				needAccountAlloc = false
			}
		}

		if plcm.nowDataNum == plcm.dataTotalNum {
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !plcm.Ss.GapEnough() && !plcm.Ss.EpochEnough() { // wait all txs to be handled
		time.Sleep(time.Second)

		if time.Since(plcm.nodeAllocLastRunTime) >= time.Duration(plcm.nodeAllocFreq)*time.Second {
			// ifchange := plcm.Ss.StopEpoch_update(stopepoch + 1)
			// if ifchange {
			// 	plcm.sl.Slog.Printf("Supervisor: StopEpoch updated to %d", plcm.epochId+1)
			// }
			if plcm.Ss.EpochEnough() {
				plcm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
				return
			}
			if params.IfNodeAlloc {
				//如果本epoch进行了账户分配，那么不进行节点分配了，直接进入下一个epoch
				if ifPlouvained[plcm.epochId] {
					plcm.nodeAllocLastRunTime = time.Now()
					plcm.sl.Slog.Printf("Supervisor: epoch %d finishs, it allocated accounts.\n", plcm.epochId)
					plcm.epochId++
					epochAfterAccAlloc = 0
				} else {
					plcm.sl.Slog.Printf("Supervisor: epoch %d begins to allocate nodes.\n", plcm.epochId)

					//在每次节点划分前更新各节点贡献值、分片性能的操作。
					plcm.updateNodeValue(time.Since(plcm.nodeAllocLastRunTime), plcm.epochId)
					plcm.plouvainState.UpdateShardPerformance(plcm.shardPerformance)
					//先判断节点分配，根据当前节点的全局贡献值、分片-节点映射、上个epoch分片的交易负载进行分配。
					//声明一个NodeAllocate对象
					shardLoad := make(map[uint64]float64)
					shardLoadEpochids := make(map[uint64]int)
					for key, val := range plcm.shardLoadHistory {
						shardLoad[key] = val[len(val)-1]
						shardLoadEpochids[key] = len(val) - 1
					}
					//将plcm.IpNodeTable除了最后一个映射外的所有映射进行节点分配
					workIptable := make(map[uint64]map[uint64]string)
					for key, val := range plcm.IpNodeTable {
						if key != params.SupervisorShard {
							workIptable[key] = val
						}
					}
					//输出workIptable映射
					plcm.SaveShardValueToCSV(plcm.epochId, true)
					plcm.sl.Slog.Printf("Supervisor: shardLoadEpochids before nodeAlloc is %v\n", shardLoadEpochids)
					plcm.sl.Slog.Printf("Supervisor: shardLoad before nodeAlloc is %v\n", shardLoad)
					plcm.sl.Slog.Printf("Supervisor: workIptable before nodeAlloc is %v\n", workIptable)
					nodeAlloc := nodeAlloca.NewNodeAllocate(plcm.nodeValueHistory.nodeSafeVaule, plcm.nodeValueHistory.nodePerformanceVaule, workIptable, shardLoad, plcm.sl)
					newWorkIptable, iferr := nodeAlloc.NodeAllocation(plcm.epochId)
					if iferr {
						needAccountAlloc = true
						plcm.sl.Slog.Printf("Supervisor: epoch %d allocate nodes err, it begins to allocate accounts.\n", plcm.epochId)
					} else {
						plcm.sl.Slog.Printf("Supervisor: workIptable after nodeAlloc is %v\n", newWorkIptable)
						//更新节点贡献值记录、节点-分片映射、分片性能
						plcm.updateValueAfterMove(newWorkIptable)
						//更新plstate内部的数据
						plcm.plouvainState.UpdateData(newWorkIptable, plcm.shardPerformance)
						//更新作恶IP
						maliciousIP = plcm.updateMaliciousIP(plcm.IpNodeTable, newWorkIptable)
						//记录节点分配完的分片值
						plcm.SaveShardValueToCSV(plcm.epochId, false)
						//发送消息广播新的作恶IP
						plcm.nodeAllocResSend(maliciousIP, plcm.epochId+1)

						//plcm.nodeAllocLastRunTime = time.Now()
						plcm.sl.Slog.Printf("Supervisor: epoch %d finishs, it allocated nodes normally.\n", plcm.epochId)
						plcm.epochId++
						epochAfterAccAlloc++
					}
					plcm.nodeAllocLastRunTime = time.Now()
					stopepoch++
				}
			}

		}
		if needAccountAlloc || time.Since(plcm.plouvainLastRunningTime) >= time.Duration(plcm.plouvainFreq)*time.Second {
			if needAccountAlloc || time.Since(plcm.nodeAllocLastRunTime) >= time.Duration(plcm.nodeAllocFreq-10)*time.Second {
				// if !params.IfNodeAlloc {
				// 	ifchange := plcm.Ss.StopEpoch_update(plcm.epochId + 1)
				// 	if ifchange {
				// 		plcm.sl.Slog.Printf("Supervisor: StopEpoch updated to %d", plcm.epochId+1)
				// 	}
				if plcm.Ss.EpochEnough() {
					plcm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
					return
				}
				//}
				plcm.plouvainLock.Lock()
				plcm.sl.Slog.Printf("Supervisor: epoch %d begins to allocate accounts.\n", plcm.epochId)

				//账户划分前也要更新各节点贡献值、分片性能的操作。
				plcm.updateNodeValue(time.Since(plcm.nodeAllocLastRunTime), plcm.epochId)
				plcm.plouvainState.UpdateShardPerformance(plcm.shardPerformance)

				//打印分片的性能值和负载
				plcm.OutputShardPerforAndLoad()
				//打印节点的安全贡献值和性能贡献值
				plcm.OutputNodeValue()

				mmap := plcm.plouvainState.PLouvain_Partition()
				//plcm.sl.Slog.Printf("Supervisor: epoch %d allocated accounts mmap:%v \n", plcm.epochId, mmap)
				plcm.plouvainMapSend(mmap)
				for key, val := range mmap {
					plcm.modifiedMap[key] = int(val)
				}
				plcm.plouvainReset()
				plcm.plouvainLock.Unlock()
				time.Sleep(10 * time.Second)
				plcm.plouvainLastRunningTime = time.Now()
				//plcm.nodeAllocLastRunTime = time.Now() //账户分配后更新节点分配时间

				ifPlouvained[plcm.epochId] = true
				plcm.sl.Slog.Printf("Supervisor: epoch %d allocated accounts successfully.\n", plcm.epochId)

				if !params.IfNodeAlloc {
					plcm.epochId++
					plcm.nodeAllocLastRunTime = time.Now()
				}
				needAccountAlloc = false
			}
		}
	}
}

// 这里注意oldIptable里包含了最后一个映射即监督节点映射，newIptable里不包含
func (plcm *PLouvainCommitteeModule) updateMaliciousIP(oldIptable, newIptable map[uint64]map[uint64]string) map[string]string {
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
func (plcm *PLouvainCommitteeModule) nodeAllocResSend(maliciousIP map[string]string, newEpochId int) {
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
	for i := uint64(0); i < uint64(len(plcm.IpNodeTable)-1); i++ {
		for j := uint64(0); j < uint64(len(plcm.IpNodeTable[i])); j++ {
			networks.TcpDial(send_msg, plcm.IpNodeTable[i][j])
		}
	}
	plcm.sl.Slog.Println("Supervisor: node allocation result message has been sent. ")
}

func (plcm *PLouvainCommitteeModule) plouvainMapSend(m map[string]uint64) {
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
		networks.TcpDial(send_msg, plcm.IpNodeTable[i][0])
	}
	plcm.sl.Slog.Println("Supervisor: all partition map message has been sent. ")
}

func (plcm *PLouvainCommitteeModule) plouvainReset() {
	plcm.plouvainState = new(louvain.PLouvainState)
	//plcm.plouvainState.Init_PLouvainState(params.Beta, params.ShardNum, plcm.shardPerformance, false, false, false, false)
	plcm.plouvainState.Init_PLouvainState(params.Beta, params.ShardNum, plcm.shardPerformance, false, true, false, false)
	for key, val := range plcm.modifiedMap {
		plcm.plouvainState.PartitionMap[key] = uint64(val)
	}
}

func (plcm *PLouvainCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	plcm.sl.Slog.Printf("Supervisor: received  blockInfo from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	if b.BlockBodyLength == 0 {
		plcm.sl.Slog.Printf("Supervisor: received BlockBodyLength=0")
		return
	}
	if b.Epoch != plcm.epochId {
		plcm.sl.Slog.Printf("Supervisor: received BlockInfo epoch is not equal to epochID in Supervisor! \n")
		return
	}
	//plcm.sl.Slog.Printf("Supervisor: starts to handle BlockInfo with %d excuted TXs from shard %d in epoch %d.\n", len(b.ExcutedTxs), b.SenderShardID, b.Epoch)
	//根据已执行交易和relay交易加权得到分片的负载
	plcm.plouvainLock.Lock()
	loadChange := float64(len(b.InnerShardTxs)) + float64(params.Beta)*float64(len(b.Relay2Txs))
	if _, ok := plcm.shardLoadHistory[b.SenderShardID]; !ok {
		plcm.shardLoadHistory[b.SenderShardID] = make([]float64, 0)
	}
	if len(plcm.shardLoadHistory[b.SenderShardID]) == b.Epoch {
		plcm.shardLoadHistory[b.SenderShardID] = append(plcm.shardLoadHistory[b.SenderShardID], loadChange)
	} else if len(plcm.shardLoadHistory[b.SenderShardID]) == b.Epoch+1 {
		plcm.shardLoadHistory[b.SenderShardID][b.Epoch] += loadChange
	} else {
		plcm.sl.Slog.Printf("Supervisor: shard %d load history length is wrong.\n", b.SenderShardID)
	}
	for _, tx := range b.InnerShardTxs {
		plcm.plouvainState.AddEdge(tx.Sender, tx.Recipient)
	}
	plcm.sl.Slog.Printf("Supervisor: %d excuted TXs from shard %d in epoch %d  are added to graph.\n", len(b.InnerShardTxs), b.SenderShardID, b.Epoch)
	plcm.plouvainLock.Unlock()
}

// 利用plcm.sl.Slog.Printf输出节点的贡献值
func (plcm *PLouvainCommitteeModule) OutputNodeValue() {
	plcm.sl.Slog.Printf("Supervisor: node safe value is following ... \n")
	for i := 0; i < len(plcm.nodeValueHistory.nodeSafeVaule); i++ {
		plcm.sl.Slog.Printf("Shard %d :", i)
		//声明临时数组将节点安全贡献值存储，然后输出数组
		//tem := make([]float32, len(plcm.nodeValueHistory.nodeSafeVaule[uint64(i)]))
		var tem []interface{}
		for j := 0; j < len(plcm.nodeValueHistory.nodeSafeVaule[uint64(i)]); j++ {
			tem = append(tem, plcm.nodeValueHistory.nodeSafeVaule[uint64(i)][uint64(j)])
		}
		plcm.sl.Slog.Printf("%v\n", tem)
	}
	plcm.sl.Slog.Printf("Supervisor: node performance value is  following ... \n")
	for i := 0; i < len(plcm.nodeValueHistory.nodePerformanceVaule); i++ {
		plcm.sl.Slog.Printf("Shard %d :", i)
		//tem := make([]float32, len(plcm.nodeValueHistory.nodePerformanceVaule[uint64(i)]))
		var tem []interface{}
		for j := 0; j < len(plcm.nodeValueHistory.nodePerformanceVaule[uint64(i)]); j++ {
			tem = append(tem, plcm.nodeValueHistory.nodePerformanceVaule[uint64(i)][uint64(j)])
		}
		plcm.sl.Slog.Printf("%v\n", tem)
	}
}

// 打印分片性能值和最新负载
func (plcm *PLouvainCommitteeModule) OutputShardPerforAndLoad() {
	plcm.sl.Slog.Printf("Supervisor: shard performance is following ...\n")
	plcm.sl.Slog.Printf("%v\n", plcm.shardPerformance)

	epochid := len(plcm.shardLoadHistory[uint64(0)]) - 1
	plcm.sl.Slog.Printf("Supervisor: shard tx num in latest epoch %d is following ...\n", epochid)
	// temLoad := make([]uint64, len(shardTxNumHistory[epochId]))
	var temLoad []interface{}
	var epochids []interface{}
	for i := 0; i < len(plcm.shardLoadHistory); i++ {
		temLoad = append(temLoad, plcm.shardLoadHistory[uint64(i)][len(plcm.shardLoadHistory[uint64(i)])-1])
		epochids = append(epochids, len(plcm.shardLoadHistory[uint64(i)])-1)
	}
	plcm.sl.Slog.Printf("epochids:%v\n", epochids)
	plcm.sl.Slog.Printf("temLoad:%v\n", temLoad)
}

// 计算分片内节点安全值的平均值作为分片的安全值，计算分片安全值的方差和平均值写入文件
func (plcm *PLouvainCommitteeModule) SaveShardValueToCSV(epochId int, ifBegin bool) {
	//计算分片安全值
	plcm.sl.Slog.Println("Supervisor: calculate shard safe value...")
	shardSafeValue := make(map[uint64]float32)
	for shardID, nodeAction := range plcm.nodeValueHistory.nodeSafeVaule {
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
		plcm.sl.Slog.Printf("Supervisor:%v\n", err)
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
