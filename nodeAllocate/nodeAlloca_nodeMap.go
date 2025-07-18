package nodeAllocate

import (
	"blockEmulator/params"
	"blockEmulator/supervisor/supervisor_log"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)

type Node struct {
	ID              int
	Security        float64
	SecurityNorm    float64 // 归一化后的安全值
	Performance     float64
	PerformanceNorm float64 // 归一化后的性能值
	IP              string
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

type RLPANodeAllocate struct {
	numNodes      int //节点数量
	numShards     int //分片数量
	numEpochNodes int //每个时期生成的节点数量
	//numReconnectNodes          int     //重连节点数量
	// numNewNodes                int     //新节点数量
	securityVarianceThreshold  float32 //安全方差阈值
	ShardTimeVarianceThreshold float32 //时间方差阈值

	workIpNodeTable          map[uint64]map[uint64]string  //分片ID->节点ID->节点IP
	NodeSafeValue            map[uint64]map[uint64]float32 //分片ID->节点ID->节点安全值
	NodeSafeValueNorm        map[uint64]map[uint64]float32 // 只存归一化结果
	NodePerformanceValue     map[uint64]map[uint64]float32 //分片ID->节点ID->节点性能值
	NodePerformanceValueNorm map[uint64]map[uint64]float32 // 只存归一化结果
	ShardLoad                map[uint64]float64

	sl *supervisor_log.SupervisorLog // to control the stop message sending

	shards []Shard
}

func NewRLPANodeAllocate(nodeSafeValue, nodePerformanceValue, nodePerformanceValueNorm, nodeSafeValueNorm map[uint64]map[uint64]float32, workIpNodeTable map[uint64]map[uint64]string, shardLoad map[uint64]float64, sl *supervisor_log.SupervisorLog) *RLPANodeAllocate {
	return &RLPANodeAllocate{
		workIpNodeTable:            workIpNodeTable,
		NodeSafeValue:              nodeSafeValue,
		NodePerformanceValue:       nodePerformanceValue,
		NodeSafeValueNorm:          nodeSafeValueNorm,
		NodePerformanceValueNorm:   nodePerformanceValueNorm,
		ShardLoad:                  shardLoad,
		sl:                         sl,
		securityVarianceThreshold:  float32(params.SecurityVarianceThreshold),
		ShardTimeVarianceThreshold: float32(params.ShardTimeVarianceThreshold),
	}
}

func (na *RLPANodeAllocate) RLPANodeAllocation(epochId int) (map[uint64]map[uint64]string, bool) {
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
	na.sl.Slog.Println("Before Shard nodes capacity")
	for i, shard := range na.shards {
		for _, node := range shard.Nodes {
			nodeCapacity := node.PerformanceNorm*0.6 + node.SecurityNorm*0.4
			na.sl.Slog.Printf("Shard %d, Node %d capacity: %.2f\n", i, node.ID, nodeCapacity)
		}
	}
	//参与本轮的节点
	EpochNodes := getEpochNodes(nodes, na.shards)
	na.numEpochNodes = len(EpochNodes)
	shardLoad, _ := getShardLoadAndPerformance(na.shards)
	group1, group2 := splitShardsByLoadSlice(shardLoad)
	iterations := 20
	err := na.IterativeSwap(group1, group2, iterations)
	if err != nil {
		na.sl.Slog.Printf("Iterative migration failed: %v\n", err)
	}
	//记录结束时间
	endTime := time.Now()
	nodeAllocTime := endTime.Sub(startTime)
	na.saveNodeShardMapping(nodeAllocTime, epochId, true)
	//记录结束时间
	//na.outputShardStats(&shards)
	//打印shards
	na.sl.Slog.Printf("After allocation, shards: %v\n", na.shards)
	for i, shard := range na.shards {
		na.sl.Slog.Printf("Shard %d: Security: %.2f, ShardTime: %.2f, Load: %.2f, Nodes: %v\n", i, shard.Security, shard.ShardTime, shard.Load, shard.Nodes)
	}
	na.sl.Slog.Println("Shard nodes capacity")
	for i, shard := range na.shards {
		for _, node := range shard.Nodes {
			nodeCapacity := node.PerformanceNorm*0.6 + node.SecurityNorm*0.4
			na.sl.Slog.Printf("Shard %d, Node %d capacity: %.2f\n", i, node.ID, nodeCapacity)
		}
	}
	// 清空并重新初始化NodeSecurityValue和NodePerformanceValue映射
	na.NodeSafeValue = make(map[uint64]map[uint64]float32)
	na.NodePerformanceValue = make(map[uint64]map[uint64]float32)
	na.workIpNodeTable = make(map[uint64]map[uint64]string)
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

func (na *RLPANodeAllocate) getShardIndexByID(shardID int) int {
	for i, shard := range na.shards {
		if shard.ID == shardID {
			return i
		}
	}
	return -1
}

func splitShardsByLoadSlice(shardLoad []float64) (group1, group2 []uint64) {
	type pair struct {
		shardID int
		load    float64
	}
	var pairs []pair
	for i, load := range shardLoad {
		pairs = append(pairs, pair{shardID: i, load: load})
	}
	// 按负载从大到小排序
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].load > pairs[j].load
	})
	half := len(pairs) / 2
	for i, p := range pairs {
		if i < half {
			group1 = append(group1, uint64(p.shardID)) // 高负载
		} else {
			group2 = append(group2, uint64(p.shardID)) // 低负载
		}
	}
	return
}

// func (na *NodeAllocate) MigrateNode(nodeID, sourceShardID, targetShardID uint64) error {
// 	srcIdx := na.getShardIndexByID(int(sourceShardID))
// 	dstIdx := na.getShardIndexByID(int(targetShardID))
// 	if srcIdx == -1 || dstIdx == -1 {
// 		return fmt.Errorf("invalid shard id")
// 	}

// 	// 找到节点
// 	var nodeToMove *Node
// 	for i, node := range na.shards[srcIdx].Nodes {
// 		if uint64(node.ID) == nodeID {
// 			nodeToMove = &na.shards[srcIdx].Nodes[i]
// 			break
// 		}
// 	}
// 	if nodeToMove == nil {
// 		return fmt.Errorf("node %d does not exist in shard %d", nodeID, sourceShardID)
// 	}

// 	// 从源分片删除
// 	na.shards[srcIdx].Nodes = removeNodeFromSlice(*nodeToMove, na.shards[srcIdx].Nodes)
// 	// 检查目标分片是否有同ID节点，如果有则分配新ID
// 	exists := false
// 	idSet := make(map[int]struct{})
// 	for _, node := range na.shards[dstIdx].Nodes {
// 		idSet[node.ID] = struct{}{}
// 		if node.ID == nodeToMove.ID {
// 			exists = true
// 		}
// 	}
// 	if exists {
// 		// 分配目标分片中没有的新ID
// 		newID := nodeToMove.ID
// 		for {
// 			newID++
// 			if _, ok := idSet[newID]; !ok {
// 				break
// 			}
// 		}
// 		nodeToMove = &Node{
// 			ID:          newID,
// 			Security:    nodeToMove.Security,
// 			Performance: nodeToMove.Performance,
// 			IP:          nodeToMove.IP,
// 		}
// 	}
// 	// 添加到目标分片
// 	na.shards[dstIdx].Nodes = append(na.shards[dstIdx].Nodes, *nodeToMove)

// 	na.sl.Slog.Printf("Migrated node %d from shard %d to shard %d\n", nodeID, sourceShardID, targetShardID)
// 	return nil
// }

// func (na *NodeAllocate) IterativeMigration(group1, group2 []uint64, iterations int) error {
// 	totalNodes := 0
// 	for _, shard := range na.shards {
// 		totalNodes += len(shard.Nodes)
// 	}
// 	avgNodes := totalNodes / len(na.shards)
// 	numShards := len(na.shards)

// 	// 随机选择起始组
// 	currentGroup := group1
// 	isGroup1 := true
// 	if rand.Intn(2) == 1 {
// 		currentGroup = group2
// 		isGroup1 = false
// 	}

// 	// 随机选择当前组中的一个分片
// 	currentShardID := int(currentGroup[rand.Intn(len(currentGroup))])
// 	shardIdx := na.getShardIndexByID(currentShardID)
// 	if shardIdx == -1 {
// 		return fmt.Errorf("invalid shard id")
// 	}

// 	// 从当前分片中随机选择一个节点（排除节点 0）
// 	var selectedNodeID uint64
// 	for _, node := range na.shards[shardIdx].Nodes {
// 		if node.ID != 0 {
// 			selectedNodeID = uint64(node.ID)
// 			break
// 		}
// 	}
// 	if selectedNodeID == 0 {
// 		na.sl.Slog.Printf("No valid node found in shard %d\n", currentShardID)
// 		return nil
// 	}

// 	// 随机选择目标组中的一个分片
// 	targetGroup := group1
// 	if isGroup1 {
// 		targetGroup = group2
// 	}
// 	targetShardID := targetGroup[rand.Intn(len(targetGroup))]

// 	// 将选中的节点迁移到目标分片
// 	err := na.MigrateNode(selectedNodeID, uint64(currentShardID), targetShardID)
// 	if err != nil {
// 		na.sl.Slog.Printf("Failed to migrate node %d from shard %d to shard %d: %v\n", selectedNodeID, currentShardID, targetShardID, err)
// 		return err
// 	}
// 	na.sl.Slog.Printf("After migrate: %+v\n", na.shards)

// 	// 开始递归迁移
// 	return na.recursiveMigration(int(targetShardID), group1, group2, iterations, avgNodes, numShards)
// }

// func (na *NodeAllocate) recursiveMigration(currentShardID int, group1, group2 []uint64, iterations int, avgNodes, numShards int) error {
// 	if iterations <= 0 {
// 		return nil // 终止条件
// 	}

// 	shardIdx := na.getShardIndexByID(currentShardID)
// 	if shardIdx == -1 {
// 		return fmt.Errorf("invalid shard id")
// 	}
// 	currentShardNodes := len(na.shards[shardIdx].Nodes)
// 	nodesToMigrate := (numShards * currentShardNodes) / avgNodes
// 	if nodesToMigrate == 0 {
// 		return nil
// 	}

// 	// 获取可迁移节点
// 	var nodesToMove []int // 存node.ID
// 	for _, node := range na.shards[shardIdx].Nodes {
// 		if node.ID != 0 {
// 			nodesToMove = append(nodesToMove, node.ID)
// 		}
// 	}
// 	rand.Shuffle(len(nodesToMove), func(i, j int) {
// 		nodesToMove[i], nodesToMove[j] = nodesToMove[j], nodesToMove[i]
// 	})
// 	if len(nodesToMove) > nodesToMigrate {
// 		nodesToMove = nodesToMove[:nodesToMigrate]
// 	}

// 	// 随机选择目标组
// 	targetGroup := group1
// 	if containsGroup(group1, uint64(currentShardID)) {
// 		targetGroup = group2
// 	}

// 	// 依次将节点迁移到目标组的分片，并递归
// 	for _, nodeID := range nodesToMove {
// 		err := na.MigrateNode(uint64(nodeID), uint64(currentShardID), targetGroup[rand.Intn(len(targetGroup))])
// 		if err != nil {
// 			na.sl.Slog.Printf("Failed to migrate node %d from shard %d: %v\n", nodeID, currentShardID, err)
// 			continue
// 		}
// 		// 递归只对本次迁入的分片
// 		return na.recursiveMigration(int(targetGroup[rand.Intn(len(targetGroup))]), group1, group2, iterations-1, avgNodes, numShards)
// 	}
// 	return nil
// }

// // 辅助函数：判断分片是否属于某个组
//
//	func containsGroup(group []uint64, shardID uint64) bool {
//		for _, id := range group {
//			if id == shardID {
//				return true
//			}
//		}
//		return false
//	}
func (na *RLPANodeAllocate) IterativeSwap(group1, group2 []uint64, iterations int) error {
	if len(group1) == 0 || len(group2) == 0 {
		return fmt.Errorf("group1 or group2 is empty")
	}
	curGroup, otherGroup := group1, group2
	if rand.Intn(2) == 1 {
		curGroup, otherGroup = group2, group1
	}
	curShardID := curGroup[rand.Intn(len(curGroup))]

	for iter := 0; iter < iterations; iter++ {
		// Step 1: 当前组curGroup的分片curShardID，按性能选节点
		curShardIdx := na.getShardIndexByID(int(curShardID))
		if curShardIdx == -1 || len(na.shards[curShardIdx].Nodes) == 0 {
			return fmt.Errorf("invalid curShardID or empty shard")
		}
		var curNodeCandidates []int
		if containsGroup(group1, curShardID) {
			// group1: 低性能
			for _, node := range na.shards[curShardIdx].Nodes {
				nodeCapacity := node.PerformanceNorm*0.6 + node.SecurityNorm*0.4
				if node.ID != 0 && nodeCapacity < 0.5 {
					curNodeCandidates = append(curNodeCandidates, node.ID)
				}
			}
		} else {
			// group2: 高性能
			for _, node := range na.shards[curShardIdx].Nodes {
				nodeCapacity := node.PerformanceNorm*0.6 + node.SecurityNorm*0.4
				if node.ID != 0 && nodeCapacity >= 0.5 {
					curNodeCandidates = append(curNodeCandidates, node.ID)
				}
			}
		}
		if len(curNodeCandidates) == 0 {
			return fmt.Errorf("no valid node in shard %d", curShardID)
		}
		curNodeID := uint64(curNodeCandidates[rand.Intn(len(curNodeCandidates))])

		// Step 1: 另一组otherGroup随机选一个分片和节点
		otherShardID := otherGroup[rand.Intn(len(otherGroup))]
		otherShardIdx := na.getShardIndexByID(int(otherShardID))
		if otherShardIdx == -1 || len(na.shards[otherShardIdx].Nodes) == 0 {
			return fmt.Errorf("invalid otherShardID or empty shard")
		}
		var otherNodeCandidates []int
		if containsGroup(group1, otherShardID) {
			// group1: 低性能
			for _, node := range na.shards[otherShardIdx].Nodes {
				nodeCapacity := node.PerformanceNorm*0.6 + node.SecurityNorm*0.4
				if node.ID != 0 && nodeCapacity < 0.5 {
					otherNodeCandidates = append(otherNodeCandidates, node.ID)
				}
			}
		} else {
			// group2: 高性能
			for _, node := range na.shards[otherShardIdx].Nodes {
				nodeCapacity := node.PerformanceNorm*0.6 + node.SecurityNorm*0.4
				if node.ID != 0 && nodeCapacity >= 0.5 {
					otherNodeCandidates = append(otherNodeCandidates, node.ID)
				}
			}
		}
		if len(otherNodeCandidates) == 0 {
			return fmt.Errorf("no valid node in shard %d", otherShardID)
		}
		otherNodeID := uint64(otherNodeCandidates[rand.Intn(len(otherNodeCandidates))])

		// 交换
		err := na.SwapNode(curNodeID, curShardID, otherNodeID, otherShardID)
		if err != nil {
			na.sl.Slog.Printf("Swap failed: %v\n", err)
			return err
		}
		na.sl.Slog.Printf("Iter %d Step1: Swapped node %d (shard %d) <-> node %d (shard %d)\n", iter+1, curNodeID, curShardID, otherNodeID, otherShardID)

		// Step 2: 以刚刚otherShardID为起点，切换组
		curShardID, curGroup, otherGroup = otherShardID, otherGroup, curGroup
	}
	return nil
}

// 判断分片是否属于某个组
func containsGroup(group []uint64, shardID uint64) bool {
	for _, id := range group {
		if id == shardID {
			return true
		}
	}
	return false
}

// // 工具函数：获取非0号节点ID列表
// func getNonZeroNodeIDs(nodes []Node) []int {
// 	var ids []int
// 	for _, node := range nodes {
// 		if node.ID != 0 {
// 			ids = append(ids, node.ID)
// 		}
// 	}
// 	return ids
// }

// 节点交换
func (na *RLPANodeAllocate) SwapNode(nodeID1, shardID1, nodeID2, shardID2 uint64) error {
	srcIdx := na.getShardIndexByID(int(shardID1))
	dstIdx := na.getShardIndexByID(int(shardID2))
	if srcIdx == -1 || dstIdx == -1 {
		return fmt.Errorf("invalid shard id")
	}
	var idx1, idx2 int = -1, -1
	for i, node := range na.shards[srcIdx].Nodes {
		if uint64(node.ID) == nodeID1 {
			idx1 = i
			break
		}
	}
	for i, node := range na.shards[dstIdx].Nodes {
		if uint64(node.ID) == nodeID2 {
			idx2 = i
			break
		}
	}
	if idx1 == -1 || idx2 == -1 {
		return fmt.Errorf("node not found in source or target shard")
	}
	// 只交换IP
	na.shards[srcIdx].Nodes[idx1].IP, na.shards[dstIdx].Nodes[idx2].IP = na.shards[dstIdx].Nodes[idx2].IP, na.shards[srcIdx].Nodes[idx1].IP
	na.shards[srcIdx].Nodes[idx1].PerformanceNorm, na.shards[dstIdx].Nodes[idx2].PerformanceNorm = na.shards[dstIdx].Nodes[idx2].PerformanceNorm, na.shards[srcIdx].Nodes[idx1].PerformanceNorm
	na.shards[srcIdx].Nodes[idx1].SecurityNorm, na.shards[dstIdx].Nodes[idx2].SecurityNorm = na.shards[dstIdx].Nodes[idx2].SecurityNorm, na.shards[srcIdx].Nodes[idx1].SecurityNorm
	return nil
}
func (na *RLPANodeAllocate) createNodes() []Node {
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
func (na *RLPANodeAllocate) generateShards(shardsInfo map[uint64]map[uint64]string, shardsLoad map[uint64]float64) []Shard {
	var shards []Shard

	na.sl.Slog.Printf("Supervisor: shardsInfo length %d\n", len(shardsInfo))
	na.sl.Slog.Println("Supervisor: shardsInfo \n", shardsInfo)
	for shardID, nodeMap := range shardsInfo {
		var nodes []Node
		for nodeID := range nodeMap {
			// 假设有逻辑来填充每个节点的安全性和性能值
			node := Node{
				ID:              int(nodeID),
				Security:        float64(na.NodeSafeValue[shardID][nodeID]),            // 从映射获取
				SecurityNorm:    float64(na.NodeSafeValueNorm[shardID][nodeID]),        // 归一化后的安全值
				Performance:     float64(na.NodePerformanceValue[shardID][nodeID]),     // 从映射获取
				PerformanceNorm: float64(na.NodePerformanceValueNorm[shardID][nodeID]), // 归一化后的性能值
				IP:              nodeMap[nodeID],
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

func (na *RLPANodeAllocate) updateShardStats() {
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

// func removeNodeFromSlice(node Node, nodes []Node) []Node {
// 	for i, n := range nodes {
// 		if n.ID == node.ID {
// 			return append(nodes[:i], nodes[i+1:]...)
// 		}
// 	}
// 	return nodes
// }

func (na *RLPANodeAllocate) saveNodeShardMapping(nodeAllocTime time.Duration, epochId int, ifEnd bool) {
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
