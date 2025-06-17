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

// CLPA committee operations
type CLPACommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	// additional variants
	curEpoch            int32
	clpaLock            sync.Mutex
	clpaGraph           *partition.CLPAState
	modifiedMap         map[string]uint64
	clpaLastRunningTime time.Time
	clpaFreq            int

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string

	// nodeValueHistory     *NodeValueHistory
	// nodeAllocLastRunTime time.Time
	// nodeAllocFreq        int
	// shardLoadHistory     map[uint64][]float64

	// epochId int
}

func NewCLPACommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum, clpaFrequency int) *CLPACommitteeModule {
	cg := new(partition.CLPAState)
	cg.Init_CLPAState(0.5, 100, params.ShardNum)

	// newnodeValueHistory := new(NodeValueHistory)
	// newnodeValueHistory.Init_NodeValueHistory(Ip_nodeTable)
	// shardLoad := make(map[uint64][]float64)
	return &CLPACommitteeModule{
		csvPath:             csvFilePath,
		dataTotalNum:        dataNum,
		batchDataNum:        batchNum,
		nowDataNum:          0,
		clpaGraph:           cg,
		modifiedMap:         make(map[string]uint64),
		clpaFreq:            clpaFrequency,
		clpaLastRunningTime: time.Time{},
		IpNodeTable:         Ip_nodeTable,
		Ss:                  Ss,
		sl:                  sl,
		curEpoch:            0,

		// nodeValueHistory:     newnodeValueHistory,
		// nodeAllocLastRunTime: time.Time{},
		// nodeAllocFreq:        params.NodeAllocFreq,
		// shardLoadHistory:     shardLoad,
		// epochId:              0,
	}
}

func (ccm *CLPACommitteeModule) HandleOtherMessage([]byte) {}

func (ccm *CLPACommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := ccm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (ccm *CLPACommitteeModule) txSending(txlist []*core.Transaction) {
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
func (ccm *CLPACommitteeModule) HandleNodeAction(content []byte) {
	// na := new(message.NodeAction)
	// na.SafeVauleInEpoch = make(map[uint64]float32)
	// na.TxinEpoch = make(map[uint64]float32)

	// err := json.Unmarshal(content, na)
	// if err != nil {
	// 	ccm.sl.Slog.Printf("Supervisor: json.Unmarshal error: %v\n", err)
	// 	log.Panic(err)
	// }
	// ccm.sl.Slog.Printf("Supervisor: begins update node value using nodeAction message.\n")
	// //epoch节点贡献值更新
	// //打印节点贡献值
	// for nodeID, safeValue := range na.SafeVauleInEpoch {
	// 	ccm.sl.Slog.Printf("Supervisor: shard %d node %d safe value is %f\n", na.ShardIndex, nodeID, safeValue)
	// 	ccm.nodeValueHistory.temSafeVaule[na.ShardIndex][nodeID] = safeValue
	// }
	// for nodeID, txValue := range na.TxinEpoch {
	// 	ccm.sl.Slog.Printf("Supervisor: shard %d node %d tx value is %f\n", na.ShardIndex, nodeID, txValue)
	// 	ccm.nodeValueHistory.temPerformanceVaule[na.ShardIndex][nodeID] = txValue
	// }
	// ccm.sl.Slog.Printf("Supervisor: have updated node value using nodeAction message.\n")
}

// func (ccm *CLPACommitteeModule) updateNodeValue(timeDurtionInEpoch time.Duration, epochId int) {
// 	ccm.sl.Slog.Printf("Supervisor: epoch %d update NodeValue...", epochId)
// 	duration := timeDurtionInEpoch * time.Nanosecond
// 	// 将time.Duration转换为秒，并转换为uint64
// 	timeInEpoch := uint64(duration.Nanoseconds()) / uint64(time.Second)
// 	if epochId == 0 {
// 		ccm.nodeValueHistory.nodeSafeVaule = ccm.nodeValueHistory.temSafeVaule
// 		for shardID, nodeAction := range ccm.nodeValueHistory.temPerformanceVaule {
// 			for nodeID, txValue := range nodeAction {
// 				initialValue := txValue / float32(timeInEpoch)
// 				ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID] = initialValue
// 				ccm.nodeValueHistory.temPerformanceVaule[shardID][nodeID] = ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID]
// 			}
// 		}
// 	} else {
// 		// 根据本epoch贡献值改变量更新节点全局的安全贡献值
// 		for shardID, nodeAction := range ccm.nodeValueHistory.temSafeVaule {
// 			for nodeID, safeValue := range nodeAction {
// 				oldVaule := ccm.nodeValueHistory.nodeSafeVaule[shardID][nodeID]
// 				ccm.nodeValueHistory.nodeSafeVaule[shardID][nodeID] = params.Alpha*oldVaule + (1-params.Alpha)*safeValue
// 				ccm.nodeValueHistory.temSafeVaule[shardID][nodeID] = ccm.nodeValueHistory.nodeSafeVaule[shardID][nodeID]
// 			}
// 		}
// 		// 更新节点全局的性能贡献值
// 		for shardID, nodeAction := range ccm.nodeValueHistory.temPerformanceVaule {
// 			for nodeID, txValue := range nodeAction {
// 				oldVaule := ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID]
// 				changeVaule := txValue / float32(timeInEpoch)
// 				ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID] = params.Alpha*oldVaule + (1-params.Alpha)*changeVaule
// 				ccm.nodeValueHistory.temPerformanceVaule[shardID][nodeID] = ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID]
// 			}
// 		}
// 	}
// 	//更新分片性能值
// }
// func (ccm *CLPACommitteeModule) updateValueAfterMove(ipmap map[string]string) {
// 	//更新节点贡献值记录
// 	ccm.sl.Slog.Printf("start to updateValueAfterMove...\n")
// 	ipSafeValue := make(map[string]float32)
// 	iptemSafeValue := make(map[string]float32)
// 	ipPerformanceValue := make(map[string]float32)
// 	iptemPerformanceValue := make(map[string]float32)
// 	for shardID, nodelist := range ccm.IpNodeTable {
// 		if shardID == params.SupervisorShard {
// 			continue
// 		}
// 		for nodeID, ip := range nodelist {
// 			ipSafeValue[ip] = ccm.nodeValueHistory.nodeSafeVaule[shardID][nodeID]
// 			iptemSafeValue[ip] = ccm.nodeValueHistory.temSafeVaule[shardID][nodeID]
// 			ipPerformanceValue[ip] = ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID]
// 			iptemPerformanceValue[ip] = ccm.nodeValueHistory.temPerformanceVaule[shardID][nodeID]
// 		}
// 	}
// 	//与plouvain略微不同，各个值直接根据新IP映射更新
// 	for shardID, nodelist := range ccm.IpNodeTable {
// 		if shardID == params.SupervisorShard {
// 			continue
// 		}
// 		for nodeID, ip := range nodelist {
// 			if ccm.nodeValueHistory.nodeSafeVaule == nil || ccm.nodeValueHistory.temSafeVaule == nil {
// 				ccm.nodeValueHistory.nodeSafeVaule = make(map[uint64]map[uint64]float32)
// 				ccm.nodeValueHistory.nodePerformanceVaule = make(map[uint64]map[uint64]float32)
// 				ccm.nodeValueHistory.temSafeVaule = make(map[uint64]map[uint64]float32)
// 				ccm.nodeValueHistory.temPerformanceVaule = make(map[uint64]map[uint64]float32)
// 			}
// 			if _, ok := ccm.nodeValueHistory.nodeSafeVaule[shardID]; !ok {
// 				ccm.nodeValueHistory.nodeSafeVaule[shardID] = make(map[uint64]float32)
// 				ccm.nodeValueHistory.nodePerformanceVaule[shardID] = make(map[uint64]float32)
// 			}
// 			if _, ok := ccm.nodeValueHistory.temSafeVaule[shardID]; !ok {
// 				ccm.nodeValueHistory.temSafeVaule[shardID] = make(map[uint64]float32)
// 				ccm.nodeValueHistory.temPerformanceVaule[shardID] = make(map[uint64]float32)
// 			}
// 			ccm.nodeValueHistory.nodeSafeVaule[shardID][nodeID] = ipSafeValue[ipmap[ip]]
// 			ccm.nodeValueHistory.temSafeVaule[shardID][nodeID] = iptemSafeValue[ipmap[ip]]
// 			ccm.nodeValueHistory.nodePerformanceVaule[shardID][nodeID] = ipPerformanceValue[ipmap[ip]]
// 			ccm.nodeValueHistory.temPerformanceVaule[shardID][nodeID] = iptemPerformanceValue[ipmap[ip]]
// 		}
// 	}
// 	ccm.sl.Slog.Printf("have done updateValueAfterMove...\n")
// }

func (ccm *CLPACommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(ccm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)
	clpaCnt := 0
	// flag := true
	// batchId := 0
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
			if ccm.clpaLastRunningTime.IsZero() {
				ccm.clpaLastRunningTime = time.Now()
			}
			// if ccm.nodeAllocLastRunTime.IsZero() {
			// 	ccm.nodeAllocLastRunTime = time.Now()
			// }
			ccm.sl.Slog.Printf("Supervisor: txlist length is %d. \n", len(txlist))
			ccm.txSending(txlist)

			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			ccm.Ss.StopGap_Reset()
			// batchId++
		}

		// if params.ShardNum > 1 && !ccm.clpaLastRunningTime.IsZero() && time.Since(ccm.clpaLastRunningTime) >= time.Duration(0.5*float64(ccm.clpaFreq))*time.Second && time.Since(ccm.clpaLastRunningTime) < time.Duration(ccm.clpaFreq)*time.Second && flag {
		// 	ccm.clpaLock.Lock()
		// 	clpaCnt++
		// 	mmap, _ := ccm.clpaGraph.CLPA_Partition()

		// 	ccm.clpaMapSend(mmap)
		// 	for key, val := range mmap {
		// 		ccm.modifiedMap[key] = val
		// 	}
		// 	ccm.clpaReset()
		// 	ccm.clpaLock.Unlock()
		// 	flag = false
		// 	ccm.sl.Slog.Println("Run CLPA Duration ")
		// 	if ccm.nowDataNum == ccm.dataTotalNum {
		// 		break
		// 	}
		// }
		// if params.IfNodeAlloc && !ccm.nodeAllocLastRunTime.IsZero() && time.Since(ccm.nodeAllocLastRunTime) >= time.Duration(ccm.nodeAllocFreq)*time.Second {
		// 	//将IpNodeTable的string类型的IP保存为旧IP，然后旧IP随机互换作为节点新的IP
		// 	// ifchange := ccm.Ss.StopEpoch_update(ccm.epochId + 1)
		// 	// if ifchange {
		// 	// 	ccm.sl.Slog.Printf("Supervisor: StopEpoch updated to %d", ccm.epochId+1)
		// 	// }
		// 	if ccm.Ss.EpochEnough() {
		// 		ccm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
		// 		return
		// 	}
		// 	ccm.sl.Slog.Printf("Supervisor: epoch %d begins to allocate nodes.\n", ccm.epochId)
		// 	//在每次节点划分前更新各节点贡献值、分片性能的操作。
		// 	ccm.updateNodeValue(time.Since(ccm.nodeAllocLastRunTime), ccm.epochId)
		// 	oldIPtable := ccm.IpNodeTable
		// 	ipList := make([]string, 0)
		// 	for key, ipmap := range oldIPtable {
		// 		if key != params.SupervisorShard {
		// 			for _, ip := range ipmap {
		// 				ipList = append(ipList, ip)
		// 			}
		// 		}
		// 	}
		// 	ipIfAlloc := make([]bool, len(ipList))
		// 	for i := 0; i < len(ipList); i++ {
		// 		ipIfAlloc[i] = false
		// 	}
		// 	//写入分片安全值到文件
		// 	ccm.SaveShardValueToCSV(ccm.epochId, true)
		// 	//建立旧IP到新IP的映射
		// 	ipMap := make(map[string]string)
		// 	for key, ipmap := range oldIPtable {
		// 		if key != params.SupervisorShard {
		// 			for _, ip := range ipmap {
		// 				for {
		// 					// 生成 [min, max] 范围内的随机整数
		// 					min := 0
		// 					max := len(ipList) - 1
		// 					randIndex := rand.Intn(max-min+1) + min
		// 					if !ipIfAlloc[randIndex] {
		// 						ipMap[ip] = ipList[randIndex]
		// 						ipIfAlloc[randIndex] = true
		// 						break
		// 					}
		// 				}
		// 			}
		// 		}
		// 	}
		// 	// //打印ipmap
		// 	// for key, val := range ipMap {
		// 	// 	ccm.sl.Slog.Printf("Supervisor: %s -> %s\n", key, val)
		// 	// }
		// 	ccm.updateValueAfterMove(ipMap)
		// 	ccm.SaveShardValueToCSV(ccm.epochId, false)
		// 	ccm.epochId++
		// 	ccm.nodeAllocResSend(ipMap, ccm.epochId)
		// 	ccm.nodeAllocLastRunTime = time.Now()
		// }
		if params.ShardNum > 1 && !ccm.clpaLastRunningTime.IsZero() && time.Since(ccm.clpaLastRunningTime) >= time.Duration(ccm.clpaFreq)*time.Second {
			// if !params.IfNodeAlloc {
			// 	// ifchange := ccm.Ss.StopEpoch_update(ccm.epochId + 1)
			// 	// if ifchange {
			// 	// 	ccm.sl.Slog.Printf("Supervisor: StopEpoch updated to %d", ccm.epochId+1)
			// 	// }
			// 	if ccm.Ss.EpochEnough() {
			// 		ccm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
			// 		return
			// 	}
			// }
			// ccm.sl.Slog.Printf("Supervisor:epoch %d begins to allocate accounts.\n", ccm.epochId)
			ccm.clpaLock.Lock()
			clpaCnt++
			mmap, _ := ccm.clpaGraph.CLPA_Partition()

			ccm.clpaMapSend(mmap)
			for key, val := range mmap {
				ccm.modifiedMap[key] = val
			}
			ccm.clpaReset()
			ccm.clpaLock.Unlock()
			time.Sleep(10 * time.Second)
			for atomic.LoadInt32(&ccm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}
			ccm.clpaLastRunningTime = time.Now()
			// if !params.IfNodeAlloc {
			// 	ccm.epochId++
			// }
			ccm.sl.Slog.Println("Next CLPA epoch begins. ")
		}

		if ccm.nowDataNum == ccm.dataTotalNum {
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !ccm.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		// if params.IfNodeAlloc && !ccm.nodeAllocLastRunTime.IsZero() && time.Since(ccm.nodeAllocLastRunTime) >= time.Duration(ccm.nodeAllocFreq)*time.Second {
		// 	//将IpNodeTable的string类型的IP保存为旧IP，然后旧IP随机互换作为节点新的IP
		// 	// ifchange := ccm.Ss.StopEpoch_update(ccm.epochId + 1)
		// 	// if ifchange {
		// 	// 	ccm.sl.Slog.Printf("Supervisor: StopEpoch updated to %d", ccm.epochId+1)
		// 	// }
		// 	if ccm.Ss.EpochEnough() {
		// 		ccm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
		// 		return
		// 	}
		// 	ccm.sl.Slog.Printf("Supervisor: epoch %d begins to allocate nodes.\n", ccm.epochId)
		// 	//在每次节点划分前更新各节点贡献值、分片性能的操作。
		// 	ccm.updateNodeValue(time.Since(ccm.nodeAllocLastRunTime), ccm.epochId)
		// 	oldIPtable := ccm.IpNodeTable
		// 	ipList := make([]string, 0)
		// 	for key, ipmap := range oldIPtable {
		// 		if key != params.SupervisorShard {
		// 			for _, ip := range ipmap {
		// 				ipList = append(ipList, ip)
		// 			}
		// 		}
		// 	}
		// 	ipIfAlloc := make([]bool, len(ipList))
		// 	for i := 0; i < len(ipList); i++ {
		// 		ipIfAlloc[i] = false
		// 	}
		// 	//写入分片安全值到文件
		// 	ccm.SaveShardValueToCSV(ccm.epochId, true)
		// 	//建立旧IP到新IP的映射
		// 	ipMap := make(map[string]string)
		// 	for key, ipmap := range oldIPtable {
		// 		if key != params.SupervisorShard {
		// 			for _, ip := range ipmap {
		// 				for {
		// 					// 生成 [min, max] 范围内的随机整数
		// 					min := 0
		// 					max := len(ipList) - 1
		// 					randIndex := rand.Intn(max-min+1) + min
		// 					if !ipIfAlloc[randIndex] {
		// 						ipMap[ip] = ipList[randIndex]
		// 						ipIfAlloc[randIndex] = true
		// 						break
		// 					}
		// 				}
		// 			}
		// 		}
		// 	}
		// 	// //打印ipmap
		// 	// for key, val := range ipMap {
		// 	// 	ccm.sl.Slog.Printf("Supervisor: %s -> %s\n", key, val)
		// 	// }
		// 	ccm.updateValueAfterMove(ipMap)
		// 	ccm.SaveShardValueToCSV(ccm.epochId, false)
		// 	ccm.epochId++
		// 	ccm.nodeAllocResSend(ipMap, ccm.epochId)
		// 	ccm.nodeAllocLastRunTime = time.Now()
		// }
		if time.Since(ccm.clpaLastRunningTime) >= time.Duration(ccm.clpaFreq)*time.Second {
			// if !params.IfNodeAlloc {
			// 	// ifchange := ccm.Ss.StopEpoch_update(ccm.epochId + 1)
			// 	// if ifchange {
			// 	// 	ccm.sl.Slog.Printf("Supervisor: StopEpoch updated to %d", ccm.epochId+1)
			// 	// }
			// 	if ccm.Ss.EpochEnough() {
			// 		ccm.sl.Slog.Printf("Supervisor: Epoch is enough, stop MsgSendingControl.\n")
			// 		return
			// 	}
			// }
			// ccm.sl.Slog.Printf("Supervisor:epoch %d begins to allocate accounts.\n", ccm.epochId)
			ccm.clpaLock.Lock()
			clpaCnt++
			mmap, _ := ccm.clpaGraph.CLPA_Partition()
			ccm.clpaMapSend(mmap)
			for key, val := range mmap {
				ccm.modifiedMap[key] = val
			}
			ccm.clpaReset()
			ccm.clpaLock.Unlock()
			time.Sleep(10 * time.Second)
			for atomic.LoadInt32(&ccm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}
			ccm.sl.Slog.Println("Next CLPA epoch begins. ")
			ccm.clpaLastRunningTime = time.Now()
			// if !params.IfNodeAlloc {
			// 	ccm.epochId++
			// }
		}
	}
}

func (ccm *CLPACommitteeModule) clpaMapSend(m map[string]uint64) {
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

// // 计算分片内节点安全值的平均值作为分片的安全值，计算分片安全值的方差和平均值写入文件
// func (ccm *CLPACommitteeModule) SaveShardValueToCSV(epochId int, ifBegin bool) {
// 	//计算分片安全值
// 	ccm.sl.Slog.Println("Supervisor: calculate shard safe value...")
// 	shardSafeValue := make(map[uint64]float32)
// 	for shardID, nodeAction := range ccm.nodeValueHistory.nodeSafeVaule {
// 		sum := float32(0)
// 		for _, safeValue := range nodeAction {
// 			sum += safeValue
// 		}
// 		average := sum / float32(len(nodeAction))
// 		shardSafeValue[shardID] = average
// 	}
// 	//计算分片安全值的平均值和方差
// 	sum := float32(0)
// 	for _, safeValue := range shardSafeValue {
// 		sum += safeValue
// 	}
// 	average := sum / float32(len(shardSafeValue))
// 	variance := float32(0)
// 	for _, safeValue := range shardSafeValue {
// 		variance += (safeValue - average) * (safeValue - average)
// 	}
// 	variance = variance / float32(len(shardSafeValue))

// 	//将分片安全值的平均值和方差以续写的方式写入文件,第一列是epochID,第二列是begin/end,第三列是平均值，第四列是方差
// 	file, err := os.OpenFile(params.DataWrite_path+"shardSafeValue.csv", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
// 	if err != nil {
// 		ccm.sl.Slog.Printf("Supervisor:%v\n", err)
// 	}
// 	defer file.Close()
// 	writer := csv.NewWriter(file)
// 	defer writer.Flush()
// 	//如果文件是空的，写入表头
// 	if fileinfo, err := file.Stat(); err == nil && fileinfo.Size() == 0 {
// 		writer.Write([]string{"epochID", "nodeAlloc's begin/end", "shardSafeValueAverage", "shardSafeValueVariance"})
// 	}
// 	if ifBegin {
// 		writer.Write([]string{strconv.Itoa(epochId), "begin", strconv.FormatFloat(float64(average), 'f', 8, 64), strconv.FormatFloat(float64(variance), 'f', 8, 64)})
// 	} else {
// 		writer.Write([]string{strconv.Itoa(epochId), "end", strconv.FormatFloat(float64(average), 'f', 8, 64), strconv.FormatFloat(float64(variance), 'f', 8, 64)})
// 	}
// }

func (ccm *CLPACommitteeModule) clpaReset() {
	ccm.clpaGraph = new(partition.CLPAState)
	ccm.clpaGraph.Init_CLPAState(0.5, 100, params.ShardNum)
	for key, val := range ccm.modifiedMap {
		ccm.clpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
}

func (ccm *CLPACommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	ccm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	if atomic.CompareAndSwapInt32(&ccm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		ccm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}
	if b.BlockBodyLength == 0 {
		return
	}
	ccm.clpaLock.Lock()
	for _, tx := range b.InnerShardTxs {
		// ccm.clpaGraph.UpdateAccountFrequency(tx.Sender, tx.Recipient)
		// if ccm.clpaGraph.IsHotAccount(tx.Sender) || ccm.clpaGraph.IsHotAccount(tx.Recipient) {
		// 	ccm.clpaGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient})
		// }
		ccm.clpaGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient})
	}
	for _, r2tx := range b.Relay2Txs {
		// ccm.clpaGraph.UpdateAccountFrequency(r2tx.Sender, r2tx.Recipient)
		// if ccm.clpaGraph.IsHotAccount(r2tx.Sender) || ccm.clpaGraph.IsHotAccount(r2tx.Recipient) {
		// 	ccm.clpaGraph.AddEdge(partition.Vertex{Addr: r2tx.Sender}, partition.Vertex{Addr: r2tx.Recipient})
		// }
		ccm.clpaGraph.AddEdge(partition.Vertex{Addr: r2tx.Sender}, partition.Vertex{Addr: r2tx.Recipient})
	}
	ccm.clpaLock.Unlock()
}

// no operation here
func (ccm *CLPACommitteeModule) OutputNodeValue() {}
func (ccm *CLPACommitteeModule) nodeAllocResSend(maliciousIP map[string]string, newEpochId int) {
	// // send node alocation result message
	// nnm := message.NodeAllocResult{
	// 	NodeMaliciousIP: maliciousIP,
	// 	EpochID:         newEpochId,
	// }
	// nnmByte, err := json.Marshal(nnm)
	// if err != nil {
	// 	log.Panic()
	// }
	// send_msg := message.MergeMessage(message.NodeAllocMsg, nnmByte)
	// // send to worker shards
	// for i := uint64(0); i < uint64(len(ccm.IpNodeTable)); i++ {
	// 	for j := uint64(0); j < uint64(len(ccm.IpNodeTable[i])); j++ {
	// 		networks.TcpDial(send_msg, ccm.IpNodeTable[i][j])
	// 	}
	// }
	// ccm.sl.Slog.Println("Supervisor: node allocation result message has been sent. ")
}
