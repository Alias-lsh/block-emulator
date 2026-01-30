package partition

import (
	"blockEmulator/params"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"
)

func TestTop50FrequentAddresses(t *testing.T) {
	const (
		fromCol    = 3
		toCol      = 4
		maxLines   = 3000000
		outputFile = "top100_frequent_addresses.csv"
	)
	counts := make(map[string]int)
	inputFile := "../" + params.DatasetFile
	file, err := os.Open(inputFile)
	if err != nil {
		t.Fatalf("无法打开文件: %v", err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	lineNumber := 0
	for {
		if maxLines > 0 && lineNumber >= maxLines {
			break
		}
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("行 %d 解析错误: %v", lineNumber, err)
		}
		lineNumber++
		if len(record) <= fromCol || len(record) <= toCol {
			continue
		}
		fromAddr := strings.ToLower(strings.TrimSpace(record[fromCol]))
		toAddr := strings.ToLower(strings.TrimSpace(record[toCol]))
		counts[fromAddr]++
		counts[toAddr]++
	}

	// 排序
	type addrStat struct {
		Addr  string
		Count int
	}
	var stats []addrStat
	for addr, cnt := range counts {
		stats = append(stats, addrStat{addr, cnt})
	}
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Count > stats[j].Count
	})

	// 输出前50
	out, err := os.Create(outputFile)
	if err != nil {
		t.Fatalf("无法创建输出文件: %v", err)
	}
	defer out.Close()
	writer := csv.NewWriter(out)
	defer writer.Flush()
	writer.Write([]string{"Address", "TotalCount"})
	for i := 0; i < 101 && i < len(stats); i++ {
		writer.Write([]string{stats[i].Addr, fmt.Sprintf("%d", stats[i].Count)})
	}
	t.Logf("前100高频地址统计完成，结果已保存到 %s", outputFile)
}
