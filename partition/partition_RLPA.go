package partition

import (
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"sync"
)

// RLPA算法状态，state of constraint label propagation algorithm
type RLPAState struct {
	NetGraph          RGraph         // 需运行RLPA算法的图
	PartitionMap      map[Vertex]int // 记录分片信息的 map，某个节点属于哪个分片
	Edges2Shard       []int          // Shard 相邻接的边数，对应论文中的 total weight of edges associated with label k
	VertexsNumInShard []int          // Shard 内节点的数目
	WeightPenalty     float64        // 权重惩罚，对应论文中的 beta
	MinEdges2Shard    int            // 最少的 Shard 邻接边数，最小的 total weight of edges associated with label k
	MaxIterations     int            // 最大迭代次数，constraint，对应论文中的\tau
	CrossShardEdgeNum int            // 跨分片边的总数
	ShardNum          int            // 分片数目
	GraphHash         []byte

	HotAccounts      map[string]bool // 存储热点账户
	AccountFrequency map[string]int  // 存储账户的交易频率
	HotAccountLock   sync.Mutex      // 锁，用于保护热点账户的更新
}

func (graph *RLPAState) Hash() []byte {
	hash := sha256.Sum256(graph.Encode())
	return hash[:]
}

func (graph *RLPAState) Encode() []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(graph)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

func (cs *RLPAState) AddEdgeWithTime(u, v Vertex, t_now int64, T int64) {
	if _, ok := cs.NetGraph.VertexSet[u]; !ok {
		cs.AddVertex(u)
	}
	if _, ok := cs.NetGraph.VertexSet[v]; !ok {
		cs.AddVertex(v)
	}
	cs.NetGraph.AddEdgeWithTime(u, v, t_now, T)
}

// 加入节点，需要将它默认归到一个分片中
func (cs *RLPAState) AddVertex(v Vertex) {
	cs.NetGraph.AddVertex(v)
	if val, ok := cs.PartitionMap[v]; !ok {
		cs.PartitionMap[v] = utils.Addr2Shard(v.Addr)
	} else {
		cs.PartitionMap[v] = val
	}
	cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
}

// 加入边，需要将它的端点（如果不存在）默认归到一个分片中
func (cs *RLPAState) AddEdge(u, v Vertex, weight int) {
	if _, ok := cs.NetGraph.VertexSet[u]; !ok {
		cs.AddVertex(u)
	}
	if _, ok := cs.NetGraph.VertexSet[v]; !ok {
		cs.AddVertex(v)
	}
	cs.NetGraph.AddEdge(u, v, weight)
}

// 复制RLPA状态
func (dst *RLPAState) CopyRLPA(src *RLPAState) {
	dst.NetGraph.CopyGraph(src.NetGraph)
	dst.PartitionMap = make(map[Vertex]int)
	for v := range src.PartitionMap {
		dst.PartitionMap[v] = src.PartitionMap[v]
	}
	dst.Edges2Shard = make([]int, src.ShardNum)
	copy(dst.Edges2Shard, src.Edges2Shard)
	dst.VertexsNumInShard = src.VertexsNumInShard
	dst.WeightPenalty = src.WeightPenalty
	dst.MinEdges2Shard = src.MinEdges2Shard
	dst.MaxIterations = src.MaxIterations
	dst.ShardNum = src.ShardNum
	// 复制热点账户信息
	dst.HotAccounts = src.HotAccounts
	dst.AccountFrequency = src.AccountFrequency

}

// 输出RLPA
func (cs *RLPAState) PrintRLPA() {
	cs.NetGraph.PrintGraph()
	println(cs.MinEdges2Shard)
	for v, item := range cs.PartitionMap {
		print(v.Addr, " ", item, "\t")
	}
	for _, item := range cs.Edges2Shard {
		print(item, " ")
	}
	println()
}

// 根据当前划分，计算 Wk，即 Edges2Shard
func (cs *RLPAState) ComputeEdges2Shard() {
	cs.Edges2Shard = make([]int, cs.ShardNum)
	interEdge := make([]int, cs.ShardNum)
	cs.MinEdges2Shard = math.MaxInt

	// 初始化
	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Edges2Shard[idx] = 0
		interEdge[idx] = 0
	}

	// 遍历网络图中所有边
	for v, neighbors := range cs.NetGraph.EdgeSet {
		// 获取节点 v 所属的shard
		vShard := cs.PartitionMap[v]
		for u := range neighbors {
			// 同上，获取节点 u 所属的shard
			uShard := cs.PartitionMap[u]
			if vShard != uShard {
				// cs.Edges2Shard[vShard] += 1
				cs.Edges2Shard[uShard] += 1
			} else {
				interEdge[uShard]++
			}
		}
	}

	// 计算全局跨分片边总数
	cs.CrossShardEdgeNum = 0
	for _, val := range cs.Edges2Shard {
		cs.CrossShardEdgeNum += val
	}
	cs.CrossShardEdgeNum /= 2

	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Edges2Shard[idx] += interEdge[idx] / 2
	}

	// 找到最小的分片边数
	for _, val := range cs.Edges2Shard {
		if cs.MinEdges2Shard > val {
			cs.MinEdges2Shard = val
		}
	}
}

// 在账户所属分片变动时，重新计算各个参数，faster
func (cs *RLPAState) changeShardRecompute(v Vertex, old int) {
	new := cs.PartitionMap[v]
	// 遍历节点v的所有邻居
	for neighbor := range cs.NetGraph.EdgeSet[v] {
		neighborShard := cs.PartitionMap[neighbor]
		if neighborShard != new && neighborShard != old {
			cs.Edges2Shard[new] += 1
			cs.Edges2Shard[old] -= 1
		} else if neighborShard == new {
			cs.Edges2Shard[old] -= 1
			cs.CrossShardEdgeNum -= 1
		} else {
			cs.Edges2Shard[new] += 1
			cs.CrossShardEdgeNum += 1
		}
	}
	cs.MinEdges2Shard = math.MaxInt
	for _, val := range cs.Edges2Shard {
		if cs.MinEdges2Shard > val {
			cs.MinEdges2Shard = val
		}
	}
}

// 设置参数
func (cs *RLPAState) Init_RLPAState(wp float64, mIter, sn int) {
	cs.WeightPenalty = wp
	cs.MaxIterations = mIter
	cs.ShardNum = sn
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)

	cs.HotAccounts = make(map[string]bool)
	cs.AccountFrequency = make(map[string]int)

	// cs.Edges2Shard = make([]int, sn)
	// cs.CrossShardEdgeNum = 0
	// cs.MinEdges2Shard = math.MaxInt
}

// 更新账户交易频率
func (cs *RLPAState) UpdateAccountFrequency(sender, recipient string) {
	cs.HotAccountLock.Lock()
	defer cs.HotAccountLock.Unlock()

	cs.AccountFrequency[sender]++
	cs.AccountFrequency[recipient]++

	// 判断是否为热点账户
	if cs.AccountFrequency[sender] > 1000 {
		cs.HotAccounts[sender] = true
	}
	if cs.AccountFrequency[recipient] > 1000 {
		cs.HotAccounts[recipient] = true
	}
}

// 判断是否为热点账户
func (cs *RLPAState) IsHotAccount(account string) bool {
	cs.HotAccountLock.Lock()
	defer cs.HotAccountLock.Unlock()

	return cs.HotAccounts[account]
}

// 初始化划分
func (cs *RLPAState) Init_Partition() {
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	for v := range cs.NetGraph.VertexSet {
		var va = v.Addr[len(v.Addr)-8:]
		num, err := strconv.ParseInt(va, 16, 64)
		if err != nil {
			log.Panic()
		}
		cs.PartitionMap[v] = int(num) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]]++
	}
	cs.ComputeEdges2Shard()
}

// 不会出现空分片的初始化划分
func (cs *RLPAState) Stable_Init_Partition() error {
	// 设置划分默认参数
	if cs.ShardNum > len(cs.NetGraph.VertexSet) {
		return errors.New("too many shards, number of shards should be less than nodes. ")
	}
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	cnt := 0
	for v := range cs.NetGraph.VertexSet {
		cs.PartitionMap[v] = int(cnt) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
		cnt++
	}
	cs.ComputeEdges2Shard() // 删掉会更快一点，但是这样方便输出（毕竟只执行一次Init，也快不了多少）
	return nil
}

// 计算 将节点 v 放入 uShard 所产生的 score
func (cs *RLPAState) getShard_score(v Vertex, uShard int) float64 {
	score := 0.0
	for neighbor, weight := range cs.NetGraph.EdgeSet[v] {
		if cs.PartitionMap[neighbor] == uShard {
			score += float64(weight)
		}
	}
	return score
}

// 计算 将节点 v 放入 uShard 所产生的 score
func (cs *RLPAState) getShard_score2(v Vertex, uShard int) float64 {
	var score float64
	// 节点 v 的出度
	v_outdegree := len(cs.NetGraph.EdgeSet[v])
	// uShard 与节点 v 相连的边数
	Edgesto_uShard := 0
	for item := range cs.NetGraph.EdgeSet[v] {
		if cs.PartitionMap[item] == uShard {
			Edgesto_uShard += 1
		}
	}
	score = float64(Edgesto_uShard) / float64(v_outdegree) * (1 - cs.WeightPenalty*float64(cs.Edges2Shard[uShard])/float64(cs.MinEdges2Shard))
	return score
}

// RLPA 划分算法
func (cs *RLPAState) RLPA_Partition() (map[string]uint64, int) {
	cs.ComputeEdges2Shard()
	fmt.Println("Before running RLPA, cross-shard edge number:", cs.CrossShardEdgeNum)
	res := make(map[string]uint64)
	updateTreshold := make(map[string]int)
	for iter := 0; iter < cs.MaxIterations; iter++ {
		for v := range cs.NetGraph.VertexSet {
			if updateTreshold[v.Addr] >= 50 {
				continue
			}
			neighborShardScore := make(map[int]float64)
			max_score := -9999.0
			vNowShard, max_scoreShard := cs.PartitionMap[v], cs.PartitionMap[v]
			for neighbor := range cs.NetGraph.EdgeSet[v] {
				uShard := cs.PartitionMap[neighbor]
				if _, computed := neighborShardScore[uShard]; !computed {
					neighborShardScore[uShard] = cs.getShard_score(v, uShard)
					if max_score < neighborShardScore[uShard] {
						max_score = neighborShardScore[uShard]
						max_scoreShard = uShard
					}
				}
			}
			if vNowShard != max_scoreShard && cs.VertexsNumInShard[vNowShard] > 1 {
				cs.PartitionMap[v] = max_scoreShard
				res[v.Addr] = uint64(max_scoreShard)
				updateTreshold[v.Addr]++
				cs.VertexsNumInShard[vNowShard]--
				cs.VertexsNumInShard[max_scoreShard]++
				cs.changeShardRecompute(v, vNowShard)
			}
		}
	}
	for sid, n := range cs.VertexsNumInShard {
		fmt.Printf("%d has vertexs: %d\n", sid, n)
	}
	cs.ComputeEdges2Shard()
	fmt.Println("After running RLPA, cross-shard edge number:", cs.CrossShardEdgeNum)
	return res, cs.CrossShardEdgeNum
}

// CLPA 划分算法
func (cs *RLPAState) CLPA_Partition() (map[string]uint64, int) {
	cs.ComputeEdges2Shard()
	fmt.Println("Before running CLPA, cross-shard edge number:", cs.CrossShardEdgeNum)
	res := make(map[string]uint64)
	updateTreshold := make(map[string]int)
	for iter := 0; iter < cs.MaxIterations; iter += 1 { // 第一层循环控制算法次数，constraint
		for v := range cs.NetGraph.VertexSet {
			if updateTreshold[v.Addr] >= 50 {
				continue
			}
			neighborShardScore := make(map[int]float64)
			max_score := -9999.0
			vNowShard, max_scoreShard := cs.PartitionMap[v], cs.PartitionMap[v]
			for u := range cs.NetGraph.EdgeSet[v] {
				uShard := cs.PartitionMap[u]
				// 对于属于 uShard 的邻居，仅需计算一次
				if _, computed := neighborShardScore[uShard]; !computed {
					neighborShardScore[uShard] = cs.getShard_score2(v, uShard)
					if max_score < neighborShardScore[uShard] {
						max_score = neighborShardScore[uShard]
						max_scoreShard = uShard
					}
				}
			}
			if vNowShard != max_scoreShard && cs.VertexsNumInShard[vNowShard] > 1 {
				cs.PartitionMap[v] = max_scoreShard
				res[v.Addr] = uint64(max_scoreShard)
				updateTreshold[v.Addr]++
				// 重新计算 VertexsNumInShard
				cs.VertexsNumInShard[vNowShard] -= 1
				cs.VertexsNumInShard[max_scoreShard] += 1
				// 重新计算Wk
				cs.changeShardRecompute(v, vNowShard)
			}
		}
	}
	for sid, n := range cs.VertexsNumInShard {
		fmt.Printf("%d has vertexs: %d\n", sid, n)
	}

	cs.ComputeEdges2Shard()
	fmt.Println("After running CLPA, cross-shard edge number:", cs.CrossShardEdgeNum)
	return res, cs.CrossShardEdgeNum
}

func (cs *RLPAState) EraseEdges() {
	cs.NetGraph.EdgeSet = make(map[Vertex]map[Vertex]float64)
}
