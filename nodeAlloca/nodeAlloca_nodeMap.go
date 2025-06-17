package nodeAlloca

import (
	"blockEmulator/params"
	"blockEmulator/supervisor/supervisor_log"
	"encoding/csv"
	"errors"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type Node struct {
	ID          int
	Security    float64
	Performance float64
	IP          string
}

type Shard struct {
	ID        int
	Security  float64
	ShardTime float64
	Load      float64
	Nodes     []Node
}

// 假设这些映射已经由外部输入得到
// var (
// 	IpNodeTable          map[uint64]map[uint64]string  //分片ID->节点ID->节点IP
// 	NodeSafeValue        map[uint64]map[uint64]float32 //分片ID->节点ID->节点安全值
// 	NodePerformanceValue map[uint64]map[uint64]float32 //分片ID->节点ID->节点性能值
// 	ShardLoad            map[uint64]uint64
// )

// const (
// 	numNodes                   = 100    //节点数量
// 	numShards                  = 8      //分片数量
// 	numEpochNodes              = 10     //每个时期生成的节点数量
// 	numReconnectNodes          = 5      //重连节点数量
// 	numNewNodes                = 5      //新节点数量
// 	securityVarianceThreshold  = 0.007  //安全方差阈值
// 	ShardTimeVarianceThreshold = 230000 //时间方差阈值
// )

type NodeAllocate struct {
	numNodes      int //节点数量
	numShards     int //分片数量
	numEpochNodes int //每个时期生成的节点数量
	//numReconnectNodes          int     //重连节点数量
	numNewNodes                int     //新节点数量
	securityVarianceThreshold  float32 //安全方差阈值
	ShardTimeVarianceThreshold float32 //时间方差阈值

	workIpNodeTable      map[uint64]map[uint64]string  //分片ID->节点ID->节点IP
	NodeSafeValue        map[uint64]map[uint64]float32 //分片ID->节点ID->节点安全值
	NodePerformanceValue map[uint64]map[uint64]float32 //分片ID->节点ID->节点性能值
	ShardLoad            map[uint64]float64

	sl *supervisor_log.SupervisorLog // to control the stop message sending

	shards []Shard
}

func NewNodeAllocate(nodeSafeValue, nodePerformanceValue map[uint64]map[uint64]float32, workIpNodeTable map[uint64]map[uint64]string, shardLoad map[uint64]float64, sl *supervisor_log.SupervisorLog) *NodeAllocate {
	return &NodeAllocate{
		workIpNodeTable:            workIpNodeTable,
		NodeSafeValue:              nodeSafeValue,
		NodePerformanceValue:       nodePerformanceValue,
		ShardLoad:                  shardLoad,
		sl:                         sl,
		securityVarianceThreshold:  float32(params.SecurityVarianceThreshold),
		ShardTimeVarianceThreshold: float32(params.ShardTimeVarianceThreshold),
	}
}

func (na *NodeAllocate) NodeAllocation(epochId int) (map[uint64]map[uint64]string, bool) {
	//nodes := generateNodes(numNodes)
	//shards := generateShards(numShards, nodes)
	// na.ShardLoad = map[uint64]uint64{
	// 	1: 3600000, // 分片ID为1的负载值
	// 	2: 6600000, // 分片ID为2的负载值
	// 	3: 5400000,
	// 	4: 3000000,
	// 	5: 3600000,
	// 	6: 6000000,
	// 	7: 8600000,
	// 	8: 4400000,
	// }

	//记录开始时间
	startTime := time.Now()
	//输入
	nodes := na.createNodes()
	na.numNodes = len(nodes)

	na.shards = na.generateShards(na.workIpNodeTable, na.ShardLoad)
	na.numShards = len(na.shards)

	//outputNodeShardStats(&shards)//输出所有节点
	na.updateShardStats()
	na.saveNodeShardMapping(time.Duration(0), epochId, false)

	//使用 na.sl.Slog中输出shards
	na.sl.Slog.Printf("Before allocation: shards: %v\n", na.shards)
	//参与本轮的节点
	EpochNodes := getEpochNodes(nodes, na.shards)
	na.numEpochNodes = len(EpochNodes)

	// 计算平均安全值和平均性能值
	averageSecurity := epochAverageSecurity(EpochNodes)
	averagePerformance := epochAveragePerformance(EpochNodes)
	//未参与的节点
	UnusedNodes := []Node{}
	for _, node := range nodes {
		found := false
		for _, epochNode := range EpochNodes {
			if node.IP == epochNode.IP {
				found = true
				break
			}
		}
		if !found {
			UnusedNodes = append(UnusedNodes, node)
		}
	}

	//暂时不考虑新节点
	na.numNewNodes = 0
	if na.numNewNodes > 0 {
		//加入新的节点
		for i := 0; i < na.numNewNodes; i++ {
			index := rand.Intn(len(UnusedNodes))
			node := UnusedNodes[index]
			if node.Security >= averageSecurity {
				shardID := getSecurityLowestShard(na.shards)
				na.shards[shardID].Nodes = append(na.shards[shardID].Nodes, node)
			} else {
				b := node.Performance >= averagePerformance
				if b {
					//shardID := getPerformanceLowestShard(&shards)
					shardID := getShardTimeHighestShard(na.shards)
					na.shards[shardID].Nodes = append(na.shards[shardID].Nodes, node)
				}
				shardID := getSecurityHighestShard(na.shards)
				na.shards[shardID].Nodes = append(na.shards[shardID].Nodes, node)
			}
			EpochNodes = append(EpochNodes, node)
			UnusedNodes = removeNodeFromSlice(node, UnusedNodes)
		}
		na.updateShardStats()
		averageSecurity = epochAverageSecurity(EpochNodes)
		averagePerformance = epochAveragePerformance(EpochNodes)
	}

	//安全值交换的次数
	cout1 := 0
	//性能值交换的次数
	cout2 := 0
	na.sl.Slog.Printf("Supervisor: averageSecurity %f,averagePerformance %f \n", averageSecurity, averagePerformance)
	securityValues := getSecurityValues(na.shards)
	na.sl.Slog.Printf("Supervisor: securityValues %v\n", securityValues)
	minsecurityVariance := calculateVariance(securityValues)
	ShardTimeValues := getShardTimeValues(na.shards)
	na.sl.Slog.Printf("Supervisor: ShardTimeValues %v\n", ShardTimeValues)
	minShardTimeVariance := calculateVariance(ShardTimeValues)
	na.sl.Slog.Printf("Before node swap, securityVariance %f,ShardTimeVariance %f \n", minsecurityVariance, minShardTimeVariance)
	// 交换节点直到安全值的方差小于阈值，记录最小方差对应的shard切片，如果超过迭代次数则选取最小方差对应的shard切片
	//复制一份shards
	minshard := DeepCopyShardArray(na.shards)
	var maxIterations1 = params.MaxSecuritySwapIteration

	for {
		if cout1 >= maxIterations1 {
			//超过迭代次数则选取最小方差对应的shard切片
			AssignShardsFromArray(minshard, na)
			break
		}
		if minsecurityVariance > float64(na.securityVarianceThreshold) {
			cout1++
			minShardID := getMinSecurityShard(na.shards)
			maxShardID := getMaxSecurityShard(na.shards)
			na.swapNodes(minShardID, maxShardID, averageSecurity)
			na.updateShardStats()

			securityVariance := calculateVariance(getSecurityValues(na.shards))
			if securityVariance < minsecurityVariance {
				minsecurityVariance = securityVariance
				minshard = DeepCopyShardArray(na.shards)
			}
		} else {
			//安全值的方差小于阈值
			AssignShardsFromArray(minshard, na)
			break
		}
	}
	ShardTimeValues = getShardTimeValues(na.shards)
	minShardTimeVariance = calculateVariance(ShardTimeValues)
	na.sl.Slog.Printf("Swapped by security %d times: securityVariance %f,ShardTimeVariance %f \n", cout1, minsecurityVariance, minShardTimeVariance)
	shardLoad, shardPerformance := getShardLoadAndPerformance(na.shards)
	na.sl.Slog.Printf("timeswap before: shardLoad %v\n", shardLoad)
	na.sl.Slog.Printf("timeswap before: shardPerformance %v\n", shardPerformance)
	na.sl.Slog.Printf("timeswap before: ShardTimeValues %v\n", ShardTimeValues)
	minsecurityVariance1 := minsecurityVariance
	minShardTimeVariance1 := minShardTimeVariance
	minshard2 := DeepCopyShardArray(na.shards)
	var maxIterations2 = params.MaxTimeSwapIteration
	validTimes := 0
	correspondSafeIncreaRate := []float64{}
	TimeDecreaseRate := []float64{}
	for {
		if cout2 >= maxIterations2 {
			AssignShardsFromArray(minshard2, na)
			break
		}
		if minShardTimeVariance > float64(na.ShardTimeVarianceThreshold) {
			cout2++
			//返回的是分片在切片中的序号，不是分片ID
			minShardID := getMinShardTimeShard(na.shards)
			maxShardID := getMaxShardTimeShard(na.shards)
			//na.sl.Slog.Printf("start swapNodes2 %d times...\n", cout2)
			iferror := na.swapNodes2Random(minShardID, maxShardID, averagePerformance, []int{})
			//iferror := na.swapNodes2(minShardID, maxShardID, averagePerformance, []int{})
			if iferror {
				na.sl.Slog.Printf("swapNodes2Random return err \n")
				//return nil, true
				continue
			} else {
				//na.sl.Slog.Printf("swapNodes2 %d times normally ends...\n", cout2)
			}
			//na.sl.Slog.Printf("swapNodes2 %d times normally ends...\n", cout2)
			na.updateShardStats()

			ShardTimeVariance := calculateVariance(getShardTimeValues(na.shards))
			nowsecurityVariance := calculateVariance(getSecurityValues(na.shards))
			//如果swap2之后安全方差增大超过一定比例则跳过
			//计算安全值方差增大的比例
			incresareRate := (nowsecurityVariance - minsecurityVariance1) / minsecurityVariance1
			if ShardTimeVariance <= minShardTimeVariance {
				//计算时间方差降低比例
				decreaseRate := (minShardTimeVariance1 - ShardTimeVariance) / minShardTimeVariance1
				//输出时间方差降低比例和安全值方差增大比例
				//na.sl.Slog.Printf("Supervisor: TimedecreaseRate %f,SafeincresareRate %f \n", decreaseRate, incresareRate)
				TimeDecreaseRate = append(TimeDecreaseRate, decreaseRate)
				correspondSafeIncreaRate = append(correspondSafeIncreaRate, incresareRate)
				if incresareRate <= params.SecurityIncreaseRateThereshold || nowsecurityVariance <= params.SecurityVariencsUpperBound {
					validTimes++
					minShardTimeVariance = ShardTimeVariance
					minshard2 = DeepCopyShardArray(na.shards)
				}
			}
		} else {
			AssignShardsFromArray(minshard2, na)
			break
		}
	} //hereee
	//输出时间方差降低比例和安全值方差增大比例
	na.sl.Slog.Printf("Swap record: TimeDecreaseRate %v \n", TimeDecreaseRate)
	na.sl.Slog.Printf("Swap record:   SafeIncreaRate %v \n", correspondSafeIncreaRate)
	minsecurityVariance = calculateVariance(getSecurityValues(na.shards))
	ShardTimeValues = getShardTimeValues(na.shards)
	shardLoad, shardPerformance = getShardLoadAndPerformance(na.shards)
	na.sl.Slog.Printf("timeswap after: shardLoad %v\n", shardLoad)
	na.sl.Slog.Printf("timeswap after: shardPerformance %v\n", shardPerformance)
	na.sl.Slog.Printf("timeswap after: ShardTimeValues %v\n", ShardTimeValues)
	na.sl.Slog.Printf("Swapped by ShardTime %d in %d times: securityVariance %f,ShardTimeVariance %f \n", validTimes, cout2, minsecurityVariance, minShardTimeVariance)

	//记录结束时间
	endTime := time.Now()
	nodeAllocTime := endTime.Sub(startTime)
	na.saveNodeShardMapping(nodeAllocTime, epochId, true)
	//记录结束时间
	//na.outputShardStats(&shards)
	//打印shards
	na.sl.Slog.Printf("After allocation, shards: %v\n", na.shards)
	// 清空并重新初始化NodeSecurityValue和NodePerformanceValue映射
	na.NodeSafeValue = make(map[uint64]map[uint64]float32)
	na.NodePerformanceValue = make(map[uint64]map[uint64]float32)
	na.workIpNodeTable = make(map[uint64]map[uint64]string)
	for i, shard := range na.shards {
		na.sl.Slog.Printf("Shard %d: Security: %.2f, ShardTime: %.2f, Load: %.2f, Nodes: %v\n", i, shard.Security, shard.ShardTime, shard.Load, shard.Nodes)
	}
	// 不需要清空IpNodeTable，因为我们会用它来查找Node的IP地址
	for _, shard := range na.shards {
		shardID := uint64(shard.ID)

		// 为当前分片ID初始化映射（如果之前不存在）
		if _, exists := na.NodeSafeValue[shardID]; !exists {
			na.NodeSafeValue[shardID] = make(map[uint64]float32)
		}
		if _, exists := na.NodePerformanceValue[shardID]; !exists {
			na.NodePerformanceValue[shardID] = make(map[uint64]float32)
		}
		if _, exists := na.workIpNodeTable[shardID]; !exists {
			na.workIpNodeTable[shardID] = make(map[uint64]string)
		}

		for _, node := range shard.Nodes {
			nodeID := uint64(node.ID)
			na.NodeSafeValue[shardID][nodeID] = float32(node.Security)
			na.NodePerformanceValue[shardID][nodeID] = float32(node.Performance)
			na.workIpNodeTable[shardID][nodeID] = node.IP
		}
	}
	return na.workIpNodeTable, false
}

// DeepCopyNode 深拷贝 Node 实例
func DeepCopyNode(original *Node) Node {
	return Node{
		ID:          original.ID,
		Security:    original.Security,
		Performance: original.Performance,
		IP:          original.IP,
	}
}

// DeepCopyShardArray 深拷贝 Shard 数组
func DeepCopyShardArray(original []Shard) []Shard {
	if original == nil {
		return nil
	}

	// 创建一个新的 Shard 数组用于存放深拷贝的 Shard 实例
	cloned := make([]Shard, len(original))
	for i, shard := range original {
		// 深拷贝每个 Node 到 Nodes 的新切片中
		nodesCloned := make([]Node, len(shard.Nodes))
		for j, node := range shard.Nodes {
			nodesCloned[j] = DeepCopyNode(&node)
		}

		// 用深拷贝的 Nodes 创建新的 Shard 实例
		cloned[i] = Shard{
			ID:        shard.ID,
			Security:  shard.Security,
			ShardTime: shard.ShardTime,
			Load:      shard.Load,
			Nodes:     nodesCloned,
		}
	}

	return cloned
}

// AssignShardsFromArray 将 Shard 数组的内容赋值给 na.shards 切片
func AssignShardsFromArray(shardArray []Shard, na *NodeAllocate) {
	// 首先，确保 na.shards 切片有足够的容量来存储 shardArray 的内容
	if cap(na.shards) < cap(shardArray) {
		na.shards = make([]Shard, len(shardArray), cap(shardArray))
	} else {
		na.shards = na.shards[:len(shardArray)]
	}

	// 复制 shardArray 的内容到 na.shards
	for i := range shardArray {
		na.shards[i] = shardArray[i]
	}
}

func (na *NodeAllocate) createNodes() []Node {
	var nodes []Node

	// 遍历每个Shard的节点
	for shardID, nodeMap := range na.workIpNodeTable {
		for nodeID, nodeIP := range nodeMap {
			// 根据当前的shardID和nodeID获取安全值和性能值
			security := na.NodeSafeValue[shardID][nodeID]
			performance := na.NodePerformanceValue[shardID][nodeID]

			// 创建一个新的Node实例并添加到切片中
			node := Node{
				ID:          int(nodeID),
				Security:    float64(security),
				Performance: float64(performance),
				IP:          nodeIP,
			}
			nodes = append(nodes, node)
		}
	}

	return nodes
}
func (na *NodeAllocate) generateShards(shardsInfo map[uint64]map[uint64]string, shardsLoad map[uint64]float64) []Shard {
	var shards []Shard

	na.sl.Slog.Printf("Supervisor: shardsInfo length %d\n", len(shardsInfo))
	for shardID, nodeMap := range shardsInfo {
		var nodes []Node
		for nodeID := range nodeMap {
			// 假设有逻辑来填充每个节点的安全性和性能值
			node := Node{
				ID:          int(nodeID),
				Security:    float64(na.NodeSafeValue[shardID][nodeID]),        // 从映射获取
				Performance: float64(na.NodePerformanceValue[shardID][nodeID]), // 从映射获取
				IP:          nodeMap[nodeID],
			}
			nodes = append(nodes, node)
		}

		shard := Shard{
			ID:    int(shardID),
			Nodes: nodes,
			Load:  shardsLoad[shardID], // 从ShardLoad映射获取负载值
		}
		shards = append(shards, shard)
	}

	na.sl.Slog.Printf("Supervisor: shards length %d\n", len(shards))
	return shards
}

func (na *NodeAllocate) updateShardStats() {
	for i := range na.shards {
		na.shards[i].Security = calculateAverageSecurity(na.shards[i].Nodes)
		na.shards[i].ShardTime = calculateAverageShardTime(na.shards[i])
	}
}

func calculateAverageSecurity(nodes []Node) float64 {
	sum := 0.0
	for _, node := range nodes {
		sum += node.Security
	}
	return sum / float64(len(nodes))
}

func calculateAveragePerformance(nodes []Node) float64 {
	sum := 0.0
	for _, node := range nodes {
		sum += node.Performance
	}
	return sum / float64(len(nodes))
}

func getSecurityLowestShard(shards []Shard) int {
	minIndex := 0
	minSecurity := shards[0].Security
	for i, shard := range shards {
		if shard.Security < minSecurity {
			minIndex = i
			minSecurity = shard.Security
		}
	}
	return minIndex
}
func getShardTimeHighestShard(shards []Shard) int {
	maxIndex := 0
	maxShardTime := shards[0].ShardTime
	for i, shard := range shards {
		if shard.ShardTime > maxShardTime {
			maxIndex = i
			maxShardTime = shard.ShardTime
		}
	}
	return maxIndex
}

func getSecurityHighestShard(shards []Shard) int {
	maxIndex := 0
	maxSecurity := shards[0].Security
	for i, shard := range shards {
		if shard.Security > maxSecurity {
			maxIndex = i
			maxSecurity = shard.Security
		}
	}
	return maxIndex
}

func (na *NodeAllocate) swapNodes(minShardID int, maxShardID int, averageSecurity float64) {
	node1, err := getRandomNodeBelowAverageSecurity(na.shards[minShardID], averageSecurity)
	if err != nil {
		na.sl.Slog.Printf("Supervisor: Error:%v \n", err)
		return
	}
	//na.sl.Slog.Printf("Supervisor: node1  %d,Security %f \n", node1.ID, node1.Security)
	node2, err := getRandomNodeAboveAverageSecurity(na.shards[maxShardID], averageSecurity)
	if err != nil {
		na.sl.Slog.Printf("Supervisor: Error:%v \n", err)
		return
	}
	//na.sl.Slog.Printf("Supervisor: node2 %d,Security %f \n", node2.ID, node2.Security)
	na.removeShardNode(node1, minShardID)
	na.removeShardNode(node2, maxShardID)
	temNodeID := node1.ID
	node1.ID = node2.ID
	node2.ID = temNodeID
	na.shards[minShardID].Nodes = append(na.shards[minShardID].Nodes, node2)
	na.shards[maxShardID].Nodes = append(na.shards[maxShardID].Nodes, node1)
}

// 寻找处理时间最少的分片的高于平均性能的节点和处理时间最长的分片的低于平均性能的节点，如果找不到就换成最高性能的节点
// 且直接找最大性能的节点、最小性能节点，丧失了随机性
func (na *NodeAllocate) swapNodes2(minShardID int, maxShardID int, averagePerformance float64, exceptShardIDList []int) bool {
	node1, node1Performance, err1 := getHighestPerformanceNode(na.shards[minShardID])
	if err1 != nil {
		na.sl.Slog.Printf("Supervisor: Error:%v \n", err1)
	}
	//na.sl.Slog.Printf("Supervisor: node1 %d ,Performance %f \n", node1.ID, node1.Performance)
	node2, node2Performance, err2 := getLowestPerformanceNode(na.shards[maxShardID])
	if err1 != nil || err2 != nil || node1Performance < node2Performance {
		if err2 != nil {
			na.sl.Slog.Printf("Supervisor: Error:%v \n", err2)
			return true
		}
		exceptShardIDList = append(exceptShardIDList, minShardID)
		minShardID, node1, node1Performance, err1 = getHighestPerformanceNode2(na.shards, exceptShardIDList)
		if err1 != nil || node1Performance < node2Performance {
			na.sl.Slog.Printf("Supervisor: Error:%v \n", err1)
			return true
		}

	}
	na.sl.Slog.Printf("Swap: node1:[S%d N%d %v] Performance %f; node2:[S%d N%d %v] Performance %f \n",
		na.shards[minShardID].ID, node1.ID, node1.IP, node1.Performance, na.shards[maxShardID].ID, node2.ID, node2.IP, node2.Performance)
	na.removeShardNode(node1, minShardID)
	na.removeShardNode(node2, maxShardID)
	temNodeID := node1.ID
	node1.ID = node2.ID
	node2.ID = temNodeID
	na.shards[minShardID].Nodes = append(na.shards[minShardID].Nodes, node2)
	na.shards[maxShardID].Nodes = append(na.shards[maxShardID].Nodes, node1)
	return false
}

// 备份，带有随机性的节点交换
func (na *NodeAllocate) swapNodes2Random(minShardID int, maxShardID int, averagePerformance float64, exceptShardIDList []int) (iferror bool) {
	//从耗时最短的分片中得到随机的一个性能值高于平均值的节点
	node1, err1 := getRandomNodeAboveAveragePerformance(na.shards[minShardID], averagePerformance)
	// if err1 != nil {
	// 	na.sl.Slog.Printf("Supervisor: Error:%v \n", err)
	// 	return
	// }
	//na.sl.Slog.Printf("Supervisor: node1 %d ,Performance %f \n", node1.ID, node1.Performance)
	//从耗时最长的分片中得到随机的一个性能值低于平均值的节点
	node2, err2 := getRandomNodeBelowAveragePerformance(na.shards[maxShardID], averagePerformance)
	// if err2 != nil {
	// 	na.sl.Slog.Printf("Supervisor: Error:%v \n", err)
	// 	return
	// }
	node1Performance := 0.0
	if err1 != nil || err2 != nil {
		//从耗时最短的分片中得到性能值最高的节点，作为新的node1
		node1, node1Performance, err1 = getHighestPerformanceNode(na.shards[minShardID])
		if err1 != nil {
			na.sl.Slog.Printf("Supervisor: Error:%v \n", err1)
			return true
		}
		//从耗时最长的分片中得到随机的一个性能值低于node1性能值的节点作为新的node2
		node2, err2 = getRandomNodeBelowAveragePerformance(na.shards[maxShardID], node1Performance)
		if err2 != nil {
			exceptShardIDList = append(exceptShardIDList, minShardID)
			//从排除了耗时最短的分片之外的其他分片中得到性能最高的节点，作为新的node1
			minShardID, node1, node1Performance, err1 = getHighestPerformanceNode2(na.shards, exceptShardIDList)
			if err1 != nil {
				na.sl.Slog.Printf("Supervisor: Error:%v \n", err1)
				return true
			}
			//从耗时最长的分片中随机选取一个性能低于node1性能值的节点作为新的node2
			node2, err2 = getRandomNodeBelowAveragePerformance(na.shards[maxShardID], node1Performance)
			if err2 != nil {
				na.sl.Slog.Printf("Supervisor: Error:%v \n", err2)
				return true
			}
		}
	}
	na.sl.Slog.Printf("Swap: node1:[S%d N%d %v] Performance %f; node2:[S%d N%d %v] Performance %f \n",
		na.shards[minShardID].ID, node1.ID, node1.IP, node1.Performance, na.shards[maxShardID].ID, node2.ID, node2.IP, node2.Performance)
	err1 = na.removeShardNode(node1, minShardID)
	if err1 != nil {
		na.sl.Slog.Printf("Supervisor: Error:%v \n", err1) //出现报错
		return true
	}
	err2 = na.removeShardNode(node2, maxShardID)
	if err2 != nil {
		na.sl.Slog.Printf("Supervisor: Error:%v \n", err1)
		return true
	}
	na.sl.Slog.Printf("Supervisor: remove node1 node2 normally\n")
	temNodeID := node1.ID
	node1.ID = node2.ID
	node2.ID = temNodeID
	na.shards[minShardID].Nodes = append(na.shards[minShardID].Nodes, node2)
	na.shards[maxShardID].Nodes = append(na.shards[maxShardID].Nodes, node1)
	return false
}

func removeNodeFromSlice(node Node, nodes []Node) []Node {
	for i, n := range nodes {
		if n.IP == node.IP {
			return append(nodes[:i], nodes[i+1:]...)
		}
	}
	return nodes
}

func (na *NodeAllocate) removeShardNode(node Node, shardIndex int) error {
	for i, n := range na.shards[shardIndex].Nodes {
		if n.IP == node.IP {
			na.shards[shardIndex].Nodes = append(na.shards[shardIndex].Nodes[:i], na.shards[shardIndex].Nodes[i+1:]...)
			return nil
		}
	}
	return errors.New("Node not found in shard")
}

func calculateVariance(values []float64) float64 {
	n := len(values)
	if n <= 1 {
		return 0.0
	}

	mean := calculateMean(values)
	variance := 0.0
	for _, value := range values {
		variance += (value - mean) * (value - mean)
	}
	variance /= float64(n - 1)

	return variance
}

func calculateMean(values []float64) float64 {
	sum := 0.0
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func getSecurityValues(shards []Shard) []float64 {
	values := make([]float64, len(shards))
	for i, shard := range shards {
		values[i] = shard.Security
	}
	return values
}

func getShardTimeValues(shards []Shard) []float64 {
	values := make([]float64, len(shards))
	for i, shard := range shards {
		values[i] = shard.ShardTime
	}
	return values
}

func getMinSecurityShard(shards []Shard) int {
	minIndex := 0
	minSecurity := shards[0].Security
	for i, shard := range shards {
		if shard.Security < minSecurity {
			minIndex = i
			minSecurity = shard.Security
		}
	}
	return minIndex
}

func getMaxSecurityShard(shards []Shard) int {
	maxIndex := 0
	maxSecurity := shards[0].Security
	for i, shard := range shards {
		if shard.Security > maxSecurity {
			maxIndex = i
			maxSecurity = shard.Security
		}
	}
	return maxIndex
}
func getMinShardTimeShard(shards []Shard) int {
	minIndex := 0
	minShardTime := shards[0].ShardTime
	for i, shard := range shards {
		if shard.ShardTime < minShardTime {
			minIndex = i
			minShardTime = shard.ShardTime
		}
	}
	return minIndex
}
func getMinShardTimeShardExceptOne(shards []Shard, exceptShardIndex int) int {
	minIndex := 0
	minShardTime := shards[0].ShardTime
	for i, shard := range shards {
		if shard.ShardTime < minShardTime && i != exceptShardIndex {
			minIndex = i
			minShardTime = shard.ShardTime
		}
	}
	return minIndex
}

// 获取除了特定两个分片外的所有分片中的性能最好的节点
func getHighestPerformanceNode2(shards []Shard, exceptShardIDList []int) (int, Node, float64, error) {
	var highestPerformanceNode Node
	highestPerformance := 0.0
	shardIndex := 0
	for i, shard := range shards {
		if contains(exceptShardIDList, i) {
			continue
		}
		for _, node := range shard.Nodes {
			if node.Performance > highestPerformance {
				highestPerformance = node.Performance
				highestPerformanceNode = node
				shardIndex = i
			}
		}
	}
	if highestPerformance == 0.0 {
		return shardIndex, Node{}, 0.0, errors.New("No nodes found in the shard")
	}
	return shardIndex, highestPerformanceNode, highestPerformance, nil
}
func contains(slice []int, item int) bool {
	for _, i := range slice {
		if i == item {
			return true
		}
	}
	return false
}
func getMaxShardTimeShard(shards []Shard) int {
	maxIndex := 0
	maxShardTime := shards[0].ShardTime
	for i, shard := range shards {
		if shard.ShardTime > maxShardTime {
			maxIndex = i
			maxShardTime = shard.ShardTime
		}
	}
	return maxIndex
}

func getMaxShardTimeShardExceptOne(shards []Shard, exceptShardIndex int) int {
	maxIndex := 0
	maxShardTime := shards[0].ShardTime
	for i, shard := range shards {
		if shard.ShardTime > maxShardTime && i != exceptShardIndex {
			maxIndex = i
			maxShardTime = shard.ShardTime
		}
	}
	return maxIndex
}
func (na *NodeAllocate) saveNodeShardMapping(nodeAllocTime time.Duration, epochId int, ifEnd bool) {
	// file, err := os.Create("./result/node_shard_mapping.csv")
	// if err != nil {
	// 	na.sl.Slog.Printf("Supervisor: Error creating file:%v \n", err)
	// 	return
	// }
	// defer file.Close()
	filePath := "./result/node_shard_mapping.csv"

	// 使用 OpenFile 打开文件，使用 O_APPEND 标志以追加模式打开
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		na.sl.Slog.Printf("Supervisor: Error opening file for append: %v\n", err)
		return
	}

	// 使用 defer 关键字确保文件在函数退出前关闭
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	fileInfo, err := file.Stat()
	if err != nil {
		na.sl.Slog.Printf("Supervisor: Error stating file: %v\n", err)
		return
	}
	if fileInfo.Size() == 0 {
		//写入表头：
		dataTitle := []string{"shardID", "nodeID", "nodeIP", "nodeSecurity", "nodePerformance", "shardSecurity", "shardTime", "shardLoad"}
		err = writer.Write(dataTitle)
		if err != nil {
			na.sl.Slog.Printf("Supervisor: Error writing to file:%v \n", err)
			return
		}
	}
	if !ifEnd {
		data := []string{"Before allocate node in epoch", strconv.Itoa(epochId)}
		err := writer.Write(data)
		if err != nil {
			na.sl.Slog.Printf("Supervisor: Error writing to file:%v \n", err)
			return
		}
	} else {
		data := []string{"After allocate node in epoch", strconv.Itoa(epochId)}
		err := writer.Write(data)
		if err != nil {
			na.sl.Slog.Printf("Supervisor: Error writing to file:%v \n", err)
			return
		}
	}
	for _, shard := range na.shards {
		for index, node := range shard.Nodes {
			if index == 0 {
				data := []string{strconv.Itoa(shard.ID), strconv.Itoa(node.ID), node.IP, strconv.FormatFloat(node.Security, 'f', 8, 64),
					strconv.FormatFloat(node.Performance, 'f', 8, 64), strconv.FormatFloat(shard.Security, 'f', 8, 64), strconv.FormatFloat(shard.ShardTime, 'f', 8, 64), strconv.FormatFloat(shard.Load, 'f', 3, 64)}
				err := writer.Write(data)
				if err != nil {
					na.sl.Slog.Printf("Supervisor: Error writing to file:%v \n", err)
					return
				}
			} else {
				data := []string{"", strconv.Itoa(node.ID), node.IP, strconv.FormatFloat(node.Security, 'f', 8, 64),
					strconv.FormatFloat(node.Performance, 'f', 8, 64), "", "", ""}
				err := writer.Write(data)
				if err != nil {
					na.sl.Slog.Printf("Supervisor: Error writing to file:%v \n", err)
					return
				}
			}
		}
	}
	if ifEnd {
		data := []string{"NodeAllocationTime", nodeAllocTime.String()}
		err := writer.Write(data)
		if err != nil {
			na.sl.Slog.Printf("Supervisor: Error writing to file:%v \n", err)
			return
		}
	}
}

func getEpochNodes(nodes []Node, shards []Shard) []Node {
	EpochNodes := []Node{}

	for _, shard := range shards {
		for _, node := range shard.Nodes {
			EpochNodes = append(EpochNodes, node)
		}
	}

	return EpochNodes
}

func epochAverageSecurity(nodes []Node) float64 {
	sum := 0.0
	for _, node := range nodes {
		sum += node.Security
	}
	return sum / float64(len(nodes))
}
func epochAveragePerformance(nodes []Node) float64 {
	sum := 0.0
	for _, node := range nodes {
		sum += node.Performance
	}
	return sum / float64(len(nodes))
}
func getRandomNodeAboveAverageSecurity(shard Shard, averageSecurity float64) (Node, error) {
	var nodesAboveAverageSecurity []Node
	for _, node := range shard.Nodes {
		if node.Security > averageSecurity {
			nodesAboveAverageSecurity = append(nodesAboveAverageSecurity, node)
		}
	}

	if len(nodesAboveAverageSecurity) == 0 {
		return Node{}, errors.New("No nodes above average security found in the shard")
	}

	randomIndex := rand.Intn(len(nodesAboveAverageSecurity))
	return nodesAboveAverageSecurity[randomIndex], nil
}
func getRandomNodeBelowAverageSecurity(shard Shard, averageSecurity float64) (Node, error) {
	var nodesBelowAverageSecurity []Node
	for _, node := range shard.Nodes {
		if node.Security < averageSecurity {
			nodesBelowAverageSecurity = append(nodesBelowAverageSecurity, node)
		}
	}

	if len(nodesBelowAverageSecurity) == 0 {
		return Node{}, errors.New("No nodes below average security found in the shard")
	}

	randomIndex := rand.Intn(len(nodesBelowAverageSecurity))
	return nodesBelowAverageSecurity[randomIndex], nil
}
func getRandomNodeAboveAveragePerformance(shard Shard, averagePerformance float64) (Node, error) {
	var nodesAboveAveragePerformance []Node
	for _, node := range shard.Nodes {
		if node.Performance > averagePerformance {
			nodesAboveAveragePerformance = append(nodesAboveAveragePerformance, node)
		}
	}

	if len(nodesAboveAveragePerformance) == 0 {
		return Node{}, errors.New("No nodes above average performance found in the shard")
	}

	randomIndex := rand.Intn(len(nodesAboveAveragePerformance))
	return nodesAboveAveragePerformance[randomIndex], nil
}
func getHighestPerformanceNode(shard Shard) (Node, float64, error) {
	var highestPerformanceNode Node = Node{}
	var highestPerformance float64 = 0.0
	if len(shard.Nodes) == 0 {
		return Node{}, 0.0, errors.New("No nodes in the shard")
	}
	for _, node := range shard.Nodes {
		if node.Performance > highestPerformance {
			highestPerformanceNode = node
			highestPerformance = node.Performance
		}
	}
	return highestPerformanceNode, highestPerformance, nil
}
func getLowestPerformanceNode(shard Shard) (Node, float64, error) {
	var lowestPerformanceNode Node = Node{}
	var lowestPerformance float64 = math.MaxFloat64
	if len(shard.Nodes) == 0 {
		return Node{}, lowestPerformance, errors.New("No nodes in the shard")
	}
	for _, node := range shard.Nodes {
		if node.Performance < lowestPerformance {
			lowestPerformanceNode = node
			lowestPerformance = node.Performance
		}
	}
	return lowestPerformanceNode, lowestPerformance, nil
}
func getRandomNodeBelowAveragePerformance(shard Shard, averagePerformance float64) (Node, error) {
	var nodesBelowAveragePerformance []Node
	for _, node := range shard.Nodes {
		if node.Performance < averagePerformance {
			nodesBelowAveragePerformance = append(nodesBelowAveragePerformance, node)
		}
	}

	if len(nodesBelowAveragePerformance) == 0 {
		return Node{}, errors.New("No nodes below average performance found in the shard")
	}

	randomIndex := rand.Intn(len(nodesBelowAveragePerformance))
	return nodesBelowAveragePerformance[randomIndex], nil
}
func calculateAverageShardTime(shard Shard) float64 {
	if len(shard.Nodes) == 0 {
		return 0
	}
	totalPerformance := 0.0
	for _, node := range shard.Nodes {
		totalPerformance += node.Performance
	}
	averageShardTime := shard.Load / totalPerformance
	return averageShardTime
}

func getShardLoadAndPerformance(shards []Shard) ([]float64, []float64) {
	if len(shards) == 0 {
		return nil, nil
	}
	loadValues := make([]float64, len(shards))
	for i, shard := range shards {
		loadValues[i] = shard.Load
	}
	performanceValues := make([]float64, len(shards))
	for i, shard := range shards {
		totalPerformance := 0.0
		for _, node := range shard.Nodes {
			totalPerformance += node.Performance
		}
		performanceValues[i] = totalPerformance
	}
	return loadValues, performanceValues
}
