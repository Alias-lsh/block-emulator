// 图的相关操作
package partition

import (
	"math"
)

// 描述当前区块链交易集合的图
type RGraph struct {
	VertexSet map[Vertex]bool               // 节点集合，其实是 set
	EdgeSet   map[Vertex]map[Vertex]float64 // 记录节点与节点间的交易以及权值
	// lock      sync.RWMutex       //锁，但是每个储存节点各自存储一份图，不需要此
	EdgeTimes map[Vertex]map[Vertex][]float64 // 记录每条边的交易时间
}

// 增加图中的点
func (g *RGraph) AddVertex(v Vertex) {
	if g.VertexSet == nil {
		g.VertexSet = make(map[Vertex]bool)
	}
	g.VertexSet[v] = true
}

// 增加图中的边
func (g *RGraph) AddEdge(u, v Vertex, weight int) {
	// 如果没有点，则增加点
	if _, ok := g.VertexSet[u]; !ok {
		g.AddVertex(u)
	}
	if _, ok := g.VertexSet[v]; !ok {
		g.AddVertex(v)
	}
	if g.EdgeSet == nil {
		g.EdgeSet = make(map[Vertex]map[Vertex]float64)
	}
	// 初始化边的集合
	if g.EdgeSet[u] == nil {
		g.EdgeSet[u] = make(map[Vertex]float64)
	}
	if g.EdgeSet[v] == nil {
		g.EdgeSet[v] = make(map[Vertex]float64)
	}
	// 无向图，添加双向边及其权值
	// g.EdgeSet[u][v] = weight
	// g.EdgeSet[v][u] = weight
	g.EdgeSet[u][v] += 1
	g.EdgeSet[v][u] += 1
}

// 增加图中的边，动态计算关联度
func (g *RGraph) AddEdgeWithTime(u, v Vertex, t_now float64, T float64) {
	if g.EdgeSet == nil {
		g.EdgeSet = make(map[Vertex]map[Vertex]float64)
	}
	if g.EdgeTimes == nil {
		g.EdgeTimes = make(map[Vertex]map[Vertex][]float64)
	}
	if g.EdgeSet[u] == nil {
		g.EdgeSet[u] = make(map[Vertex]float64)
	}
	if g.EdgeSet[v] == nil {
		g.EdgeSet[v] = make(map[Vertex]float64)
	}
	if g.EdgeTimes[u] == nil {
		g.EdgeTimes[u] = make(map[Vertex][]float64)
	}
	if g.EdgeTimes[v] == nil {
		g.EdgeTimes[v] = make(map[Vertex][]float64)
	}

	// 记录交易时间
	g.EdgeTimes[u][v] = append(g.EdgeTimes[u][v], t_now)
	g.EdgeTimes[v][u] = append(g.EdgeTimes[v][u], t_now)
	// fmt.Println("EdgeTimes for", u, "->", v, ":", g.EdgeTimes[u][v])
	// 计算关联度
	association := g.calculateAssociation(g.EdgeTimes[u][v], t_now, T)
	// 更新边权值
	g.EdgeSet[u][v] = association
	g.EdgeSet[v][u] = association
}

// 增加图中的边，动态计算关联度
func (g *RGraph) AddEdgeWithTime2(u, v Vertex, t_now float64) {
	if g.EdgeSet == nil {
		g.EdgeSet = make(map[Vertex]map[Vertex]float64)
	}
	if g.EdgeTimes == nil {
		g.EdgeTimes = make(map[Vertex]map[Vertex][]float64)
	}
	if g.EdgeSet[u] == nil {
		g.EdgeSet[u] = make(map[Vertex]float64)
	}
	if g.EdgeSet[v] == nil {
		g.EdgeSet[v] = make(map[Vertex]float64)
	}
	if g.EdgeTimes[u] == nil {
		g.EdgeTimes[u] = make(map[Vertex][]float64)
	}
	if g.EdgeTimes[v] == nil {
		g.EdgeTimes[v] = make(map[Vertex][]float64)
	}

	// 记录交易时间
	g.EdgeTimes[u][v] = append(g.EdgeTimes[u][v], t_now)
	g.EdgeTimes[v][u] = append(g.EdgeTimes[v][u], t_now)
	// fmt.Println("EdgeTimes for", u, "->", v, ":", g.EdgeTimes[u][v])
	// 更新边权值
	g.EdgeSet[u][v] += 1
	g.EdgeSet[v][u] += 1
}

// 计算关联度
func (g *RGraph) calculateAssociation(times []float64, t_now float64, T float64) float64 {
	association := 0.0
	count := 0.0
	for _, t_i := range times {
		if t_now-t_i <= T { // 仅考虑时间窗口 T 内的交易
			association += math.Exp(-0.1 * (t_now - t_i))
			count++
		}
	}
	// fmt.Println("association: ", association*count)
	// return association / T
	return association * count
}

// 复制图
func (dst *RGraph) CopyGraph(src RGraph) {
	dst.VertexSet = make(map[Vertex]bool)
	for v := range src.VertexSet {
		dst.VertexSet[v] = true
	}
	if src.EdgeSet != nil {
		dst.EdgeSet = make(map[Vertex]map[Vertex]float64)
		for u, neighbors := range src.EdgeSet {
			dst.EdgeSet[u] = make(map[Vertex]float64)
			for v, weight := range neighbors {
				dst.EdgeSet[u][v] = weight
			}
		}
	}
}

// 输出图
func (g RGraph) PrintGraph() {
	for v := range g.VertexSet {
		print(v.Addr, " ")
		print("edges:")
		if neighbors, ok := g.EdgeSet[v]; ok {
			for u, weight := range neighbors {
				print(" ", u.Addr, "(weight:", weight, ")\t")
			}
		}
		println()
	}
	println()
}
