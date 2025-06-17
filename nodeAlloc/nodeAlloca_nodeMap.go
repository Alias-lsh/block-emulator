package nodeAlloc

import (
	"encoding/csv"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
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

	IpNodeTable          map[uint64]map[uint64]string  //分片ID->节点ID->节点IP
	NodeSafeValue        map[uint64]map[uint64]float32 //分片ID->节点ID->节点安全值
	NodePerformanceValue map[uint64]map[uint64]float32 //分片ID->节点ID->节点性能值
	ShardLoad            map[uint64]float64
}

func NodeAllocation(nodeSafeValue, nodePerformanceValue map[uint64]map[uint64]float32, ipNodeTable map[uint64]map[uint64]string, shardLoad map[uint64]float64) map[uint64]map[uint64]string {
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
	na := NodeAllocate{}
	na.IpNodeTable = ipNodeTable
	na.NodeSafeValue = nodeSafeValue
	na.NodePerformanceValue = nodePerformanceValue
	na.ShardLoad = shardLoad
	na.securityVarianceThreshold = 0.007
	na.ShardTimeVarianceThreshold = 230000

	//输入
	nodes := na.createNodes()
	na.numNodes = len(nodes)
	shards := na.generateShards(na.IpNodeTable, na.ShardLoad)
	na.numShards = len(shards)

	fmt.Println(shards)
	//outputNodeShardStats(&shards)//输出所有节点
	updateShardStats(&shards)
	saveNodeShardMapping(&shards)
	//参与本轮的节点
	EpochNodes := getEpochNodes(nodes, shards)
	na.numEpochNodes = len(EpochNodes)

	// 计算平均安全值和平均性能值
	averageSecurity := epochAverageSecurity(EpochNodes)
	averagePerformance := epochAveragePerformance(EpochNodes)
	//未参与的节点
	UnusedNodes := []Node{}
	for _, node := range nodes {
		found := false
		for _, epochNode := range EpochNodes {
			if node.ID == epochNode.ID {
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
				shardID := getSecurityLowestShard(&shards)
				shards[shardID].Nodes = append(shards[shardID].Nodes, node)
			} else {
				b := node.Performance >= averagePerformance
				if b {
					//shardID := getPerformanceLowestShard(&shards)
					shardID := getShardTimeHighestShard(&shards)
					shards[shardID].Nodes = append(shards[shardID].Nodes, node)
				}
				shardID := getSecurityHighestShard(&shards)
				shards[shardID].Nodes = append(shards[shardID].Nodes, node)
			}
			EpochNodes = append(EpochNodes, node)
			UnusedNodes = removeNodeFromSlice(node, UnusedNodes)
		}
		updateShardStats(&shards)
		averageSecurity = epochAverageSecurity(EpochNodes)
		averagePerformance = epochAveragePerformance(EpochNodes)
	}

	//安全值交换的次数
	cout1 := 0
	//性能值交换的次数
	cout2 := 0
	fmt.Println("averageSecurity\t", averageSecurity, "averagePerformance\t", averagePerformance)
	securityVariance := calculateVariance(getSecurityValues(&shards))
	ShardTimeVariance := calculateVariance(getShardTimeValues(&shards))
	fmt.Println("securityVariance\t", securityVariance, "ShardTimeVariance\t", ShardTimeVariance)
	for {
		securityVariance := calculateVariance(getSecurityValues(&shards))
		if securityVariance > float64(na.securityVarianceThreshold) {
			cout1++
			minShardID := getMinSecurityShard(&shards)
			maxShardID := getMaxSecurityShard(&shards)
			swapNodes(&shards[minShardID], &shards[maxShardID], averageSecurity)
			updateShardStats(&shards)
			//securityVariance := calculateVariance(getSecurityValues(&shards)) //只作为输出看一下，可以删掉
			//fmt.Println("cout1\t", cout1, "securityVariance\t", securityVariance)
		} else {
			break
		}
	}

	for {
		ShardTimeVariance := calculateVariance(getShardTimeValues(&shards))
		//fmt.Println("ShardTimeVariance\t", ShardTimeVariance)
		if ShardTimeVariance > float64(na.ShardTimeVarianceThreshold) {
			cout2++
			minShardID := getMinShardTimeShard(&shards)
			fmt.Println("minShardID\t", minShardID)
			maxShardID := getMaxShardTimeShard(&shards)
			fmt.Println("maxShardID\t", maxShardID)
			swapNodes2(&shards[minShardID], &shards[maxShardID], averagePerformance)
			updateShardStats(&shards)
			ShardTimeVariance := calculateVariance(getShardTimeValues(&shards)) //只作为输出看一下，可以删掉
			fmt.Println("cout2\t", cout2, "ShardTimeVariance\t", ShardTimeVariance)
		} else {
			break
		}
	}
	ShardTimeVariance = calculateVariance(getShardTimeValues(&shards))
	fmt.Println("performanceVariance\t", ShardTimeVariance)
	securityVariance = calculateVariance(getSecurityValues(&shards))
	fmt.Println("securityVariance\t", securityVariance)
	saveNodeShardMapping(&shards)
	outputShardStats(&shards)
	fmt.Println("cout1\t", cout1, "cout2\t", cout2)

	//建立IP到安全值、性能值的映射
	ipSafeValue := make(map[string]float32)
	ipPerformanceValue := make(map[string]float32)
	for shardID, nodeMap := range na.IpNodeTable {
		for nodeID, ip := range nodeMap {
			ipSafeValue[ip] = na.NodeSafeValue[shardID][nodeID]
			ipPerformanceValue[ip] = na.NodePerformanceValue[shardID][nodeID]
		}
	}

	// 清空并重新初始化NodeSecurityValue和NodePerformanceValue映射
	na.NodeSafeValue = make(map[uint64]map[uint64]float32)
	na.NodePerformanceValue = make(map[uint64]map[uint64]float32)
	na.IpNodeTable = make(map[uint64]map[uint64]string)
	// 不需要清空IpNodeTable，因为我们会用它来查找Node的IP地址
	for _, shard := range shards {
		shardID := uint64(shard.ID)

		// 为当前分片ID初始化映射（如果之前不存在）
		if _, exists := na.NodeSafeValue[shardID]; !exists {
			na.NodeSafeValue[shardID] = make(map[uint64]float32)
		}
		if _, exists := na.NodePerformanceValue[shardID]; !exists {
			na.NodePerformanceValue[shardID] = make(map[uint64]float32)
		}
		if _, exists := na.IpNodeTable[shardID]; !exists {
			na.IpNodeTable[shardID] = make(map[uint64]string)
		}

		for _, node := range shard.Nodes {
			nodeID := uint64(node.ID)
			na.NodeSafeValue[shardID][nodeID] = float32(node.Security)
			na.NodePerformanceValue[shardID][nodeID] = float32(node.Performance)
			na.IpNodeTable[shardID][nodeID] = node.IP
		}
	}
	return na.IpNodeTable
}

func (na *NodeAllocate) createNodes() []Node {
	var nodes []Node

	// 遍历每个Shard的节点
	for shardID, nodeMap := range na.IpNodeTable {
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

	for shardID, nodeMap := range shardsInfo {
		var nodes []Node
		for nodeID := range nodeMap {
			// 假设有逻辑来填充每个节点的安全性和性能值
			node := Node{
				ID:          int(nodeID),
				Security:    float64(na.NodeSafeValue[shardID][nodeID]),        // 从映射获取
				Performance: float64(na.NodePerformanceValue[shardID][nodeID]), // 从映射获取
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

	return shards
}

func updateShardStats(shards *[]Shard) {
	for i := range *shards {
		shard := &(*shards)[i]
		shard.Security = calculateAverageSecurity(shard.Nodes)
		shard.ShardTime = calculateAverageShardTime(shard)
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

// func isNode```
func isNodePresent(nodeID int, shards []Shard) bool {
	for _, shard := range shards {
		for _, node := range shard.Nodes {
			if node.ID == nodeID {
				return true
			}
		}
	}
	return false
}

func getSecurityLowestShard(shards *[]Shard) int {
	minIndex := 0
	minSecurity := (*shards)[0].Security
	for i, shard := range *shards {
		if shard.Security < minSecurity {
			minIndex = i
			minSecurity = shard.Security
		}
	}
	return minIndex
}
func getShardTimeHighestShard(shards *[]Shard) int {
	maxIndex := 0
	maxShardTime := (*shards)[0].ShardTime
	for i, shard := range *shards {
		if shard.ShardTime > maxShardTime {
			maxIndex = i
			maxShardTime = shard.ShardTime
		}
	}
	return maxIndex
}

func getSecurityHighestShard(shards *[]Shard) int {
	maxIndex := 0
	maxSecurity := (*shards)[0].Security
	for i, shard := range *shards {
		if shard.Security > maxSecurity {
			maxIndex = i
			maxSecurity = shard.Security
		}
	}
	return maxIndex
}

func swapNodes(shard1 *Shard, shard2 *Shard, averageSecurity float64) {
	node1, err := getRandomNodeBelowAverageSecurity(*shard1, averageSecurity)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("node1:", node1.ID, "Security", node1.Security)
	node2, err := getRandomNodeAboveAverageSecurity(*shard2, averageSecurity)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("node2:", node2.ID, "Security", node2.Security)
	shard1.Nodes = removeNodeFromSlice(node1, shard1.Nodes)
	shard1.Nodes = append(shard1.Nodes, node2)

	shard2.Nodes = removeNodeFromSlice(node2, shard2.Nodes)
	shard2.Nodes = append(shard2.Nodes, node1)
}

func swapNodes2(shard1 *Shard, shard2 *Shard, averagePerformance float64) {
	node1, err := getRandomNodeAboveAveragePerformance(*shard1, averagePerformance)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("node1:", node1.ID, "Performance", node1.Performance)
	node2, err := getRandomNodeBelowAveragePerformance(*shard2, averagePerformance)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("node2:", node2.ID, "Performance", node2.Performance)
	shard1.Nodes = removeNodeFromSlice(node1, shard1.Nodes)
	shard1.Nodes = append(shard1.Nodes, node2)

	shard2.Nodes = removeNodeFromSlice(node2, shard2.Nodes)
	shard2.Nodes = append(shard2.Nodes, node1)
}

func removeNodeFromSlice(node Node, nodes []Node) []Node {
	for i, n := range nodes {
		if n.ID == node.ID {
			return append(nodes[:i], nodes[i+1:]...)
		}
	}
	return nodes
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

func getSecurityValues(shards *[]Shard) []float64 {
	values := make([]float64, len(*shards))
	for i, shard := range *shards {
		values[i] = shard.Security
	}
	return values
}

func getShardTimeValues(shards *[]Shard) []float64 {
	values := make([]float64, len(*shards))
	for i, shard := range *shards {
		values[i] = shard.ShardTime
	}
	return values
}

func getMinSecurityShard(shards *[]Shard) int {
	minIndex := 0
	minSecurity := (*shards)[0].Security
	for i, shard := range *shards {
		if shard.Security < minSecurity {
			minIndex = i
			minSecurity = shard.Security
		}
	}
	return minIndex
}

func getMaxSecurityShard(shards *[]Shard) int {
	maxIndex := 0
	maxSecurity := (*shards)[0].Security
	for i, shard := range *shards {
		if shard.Security > maxSecurity {
			maxIndex = i
			maxSecurity = shard.Security
		}
	}
	return maxIndex
}

func getMinShardTimeShard(shards *[]Shard) int {
	minIndex := 0
	minShardTime := (*shards)[0].ShardTime
	for i, shard := range *shards {
		if shard.ShardTime < minShardTime {
			minIndex = i
			minShardTime = shard.ShardTime

		}
	}
	return minIndex
}

func getMaxShardTimeShard(shards *[]Shard) int {
	maxIndex := 0
	maxShardTime := (*shards)[0].ShardTime
	for i, shard := range *shards {
		if shard.ShardTime > maxShardTime {
			maxIndex = i
			maxShardTime = shard.ShardTime
		}
	}
	return maxIndex
}

func saveNodeShardMapping(shards *[]Shard) {
	file, err := os.Create("node_shard_mapping.csv")
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, shard := range *shards {
		for _, node := range shard.Nodes {
			data := []string{strconv.Itoa(node.ID), strconv.Itoa(shard.ID), strconv.FormatFloat(node.Security, 'f', 2, 64),
				strconv.Itoa(int(node.Performance))}
			err := writer.Write(data)
			if err != nil {
				fmt.Println("Error writing to file:", err)
				return
			}
		}
	}
}

func outputShardStats(shards *[]Shard) {
	fmt.Println("Shard\tSecurity\tShardTime\tLoad")
	for _, shard := range *shards {
		fmt.Printf("%d\t%.2f\t\t%.2f\t%.2f\n", shard.ID, shard.Security, shard.ShardTime, shard.Load)
	}
}
func outputNodeShardStats(shards *[]Shard) {
	fmt.Println("Node\tShard\tSecurity\tPerformance")
	for _, shard := range *shards {
		for _, node := range shard.Nodes {
			fmt.Printf("%d\t%d\t%.2f\t\t%.2f\n", node.ID, shard.ID, node.Security, node.Performance)
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
		return Node{}, errors.New("No nodes above average security found in the shard")
	}

	randomIndex := rand.Intn(len(nodesAboveAveragePerformance))
	return nodesAboveAveragePerformance[randomIndex], nil
}
func getRandomNodeBelowAveragePerformance(shard Shard, averagePerformance float64) (Node, error) {
	var nodesBelowAveragePerformance []Node
	for _, node := range shard.Nodes {
		if node.Performance < averagePerformance {
			nodesBelowAveragePerformance = append(nodesBelowAveragePerformance, node)
		}
	}

	if len(nodesBelowAveragePerformance) == 0 {
		return Node{}, errors.New("No nodes below average security found in the shard")
	}

	randomIndex := rand.Intn(len(nodesBelowAveragePerformance))
	return nodesBelowAveragePerformance[randomIndex], nil
}
func calculateAverageShardTime(shard *Shard) float64 {
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
