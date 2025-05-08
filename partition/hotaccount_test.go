package partition

import (
	"blockEmulator/params"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"testing"
)

// 统计结果结构体
type TransactionStats struct {
	AddressCounts map[string]int    // 地址交易次数
	PairCounts    map[[2]string]int // 地址对交易次数
}

func NewTransactionStats() *TransactionStats {
	return &TransactionStats{
		AddressCounts: make(map[string]int),
		PairCounts:    make(map[[2]string]int),
	}
}

// 处理CSV文件
func ProcessCSVFile(filename string, fromCol, toCol int) (*TransactionStats, error) {
	stats := NewTransactionStats()

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("无法打开文件: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	lineNumber := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("行 %d 解析错误: %v", lineNumber, err)
			continue
		}
		lineNumber++

		// 检查列数是否足够
		if len(record) <= fromCol || len(record) <= toCol {
			log.Printf("行 %d 列数不足，跳过", lineNumber)
			continue
		}

		fromAddr := strings.ToLower(strings.TrimSpace(record[fromCol]))
		toAddr := strings.ToLower(strings.TrimSpace(record[toCol]))

		// 统计地址交易次数
		stats.AddressCounts[fromAddr]++
		stats.AddressCounts[toAddr]++

		// 生成排序后的地址对
		pair := SortAddressPair(fromAddr, toAddr)
		stats.PairCounts[pair]++
	}

	return stats, nil
}

// 生成排序后的地址对
func SortAddressPair(a, b string) [2]string {
	if a < b {
		return [2]string{a, b}
	}
	return [2]string{b, a}
}

// 将统计结果写入CSV文件
func WriteStatsToCSV(stats *TransactionStats, outputPrefix string) error {
	// 写入地址统计
	if err := writeAddressStats(stats.AddressCounts, outputPrefix+"_addresses.csv"); err != nil {
		return err
	}

	// 写入地址对统计
	return writePairStats(stats.PairCounts, outputPrefix+"_pairs.csv")
}

func writeAddressStats(counts map[string]int, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入标题
	if err := writer.Write([]string{"Address", "TransactionCount"}); err != nil {
		return err
	}

	// 排序并写入数据
	sorted := sortAddressCounts(counts)
	for _, item := range sorted {
		if err := writer.Write([]string{
			item.Address,
			fmt.Sprintf("%d", item.Count),
		}); err != nil {
			return err
		}
	}

	return nil
}

func writePairStats(counts map[[2]string]int, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入标题
	if err := writer.Write([]string{"AddressA", "AddressB", "TransactionCount"}); err != nil {
		return err
	}

	// 排序并写入数据
	sorted := sortPairCounts(counts)
	for _, item := range sorted {
		if err := writer.Write([]string{
			item.Pair[0],
			item.Pair[1],
			fmt.Sprintf("%d", item.Count),
		}); err != nil {
			return err
		}
	}

	return nil
}

// 排序辅助结构
type AddressCount struct {
	Address string
	Count   int
}

type PairCount struct {
	Pair  [2]string
	Count int
}

func sortAddressCounts(counts map[string]int) []AddressCount {
	sorted := make([]AddressCount, 0, len(counts))
	for addr, count := range counts {
		sorted = append(sorted, AddressCount{addr, count})
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Count > sorted[j].Count
	})

	return sorted
}

func sortPairCounts(counts map[[2]string]int) []PairCount {
	sorted := make([]PairCount, 0, len(counts))
	for pair, count := range counts {
		sorted = append(sorted, PairCount{pair, count})
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Count > sorted[j].Count
	})

	return sorted
}

func TestHotaccount(t *testing.T) {
	// 配置参数
	const (
		outputPrefix = "results"
		fromCol      = 3 // 发送方地址列索引
		toCol        = 4 // 接收方地址列索引
	)
	inputFile := "../" + params.DatasetFile
	// 处理CSV文件
	stats, err := ProcessCSVFile(inputFile, fromCol, toCol)
	if err != nil {
		log.Fatalf("处理文件失败: %v", err)
	}

	// 写入结果文件
	if err := WriteStatsToCSV(stats, outputPrefix); err != nil {
		log.Fatalf("写入结果失败: %v", err)
	}

	fmt.Printf("处理完成！统计结果：\n")
	fmt.Printf("- 唯一地址数量: %d\n", len(stats.AddressCounts))
	fmt.Printf("- 地址对数量: %d\n", len(stats.PairCounts))
	fmt.Printf("- 结果文件已保存为：%s_addresses.csv 和 %s_pairs.csv\n", outputPrefix, outputPrefix)
}
