package params

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

type ChainConfig struct {
	ChainID        uint64
	NodeID         uint64
	ShardID        uint64
	Nodes_perShard uint64
	ShardNums      uint64
	BlockSize      uint64
	BlockInterval  uint64
	InjectSpeed    uint64

	// used in transaction relaying, useless in brokerchain mechanism
	MaxRelayBlockSize uint64
}

var (
	SupervisorShard    = uint64(2147483647)
	Init_Balance, _    = new(big.Int).SetString("100000000000000000000000000000000000000000000", 10)
	IPmap_nodeTable    = make(map[uint64]map[uint64]string)
	CommitteeMethod    = []string{"CLPA_Broker", "CLPA", "Broker", "Relay", "RLPA", "P-Louvain"}
	MeasureBrokerMod   = []string{"TPS_Broker", "TCL_Broker", "CrossTxRate_Broker", "TxNumberCount_Broker"}
	MeasureRelayMod    = []string{"TPS_Relay", "TCL_Relay", "CrossTxRate_Relay", "TxNumberCount_Relay"}
	MeasurePLouvainMod = []string{"TPS_PLouvain", "TCL_PLouvain", "CrossTxRate_PLouvain", "TxNumberCount_PLouvain"}
)

// extractPortFromAddress 从IP:端口字符串中提取端口号
func ExtractPortFromAddress(address string) (int, error) {
	// 使用冒号分隔IP和端口号
	parts := strings.SplitN(address, ":", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("地址格式错误，应为'IP:端口号'")
	}

	// 提取端口号字符串
	portStr := parts[1]

	// 将端口号字符串转换为int
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, fmt.Errorf("转换端口号失败: %v", err)
	}

	return port, nil
}
