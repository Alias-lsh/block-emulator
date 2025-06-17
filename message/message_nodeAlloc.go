package message

var (
	NodeAllocMsg MessageType = "NodeAllocResult"
)

type NodeAllocResult struct {
	//即新的物理IP-作恶IP映射表
	NodeMaliciousIP map[string]string
	EpochID         int
}
