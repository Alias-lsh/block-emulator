package partition

import (
	"blockEmulator/params"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestAddressSendRecvCount(t *testing.T) {
	const (
		outputFile = "address_send_recv_count2.csv"
		fromCol    = 3 // 发送方地址列索引
		toCol      = 4 // 接收方地址列索引
		maxLines   = 3000000
	)
	inputFile := "../" + params.DatasetFile

	// 统计发送方和接收方次数
	sendCount := make(map[string]int)
	recvCount := make(map[string]int)

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
		sendCount[fromAddr]++
		recvCount[toAddr]++
	}

	// 写入结果到CSV
	out, err := os.Create(outputFile)
	if err != nil {
		t.Fatalf("无法创建输出文件: %v", err)
	}
	defer out.Close()
	writer := csv.NewWriter(out)
	defer writer.Flush()
	// 标题
	if err := writer.Write([]string{"Address", "SendCount", "RecvCount", "TotalCount"}); err != nil {
		t.Fatalf("写入标题失败: %v", err)
	}
	// 合并所有地址
	addressSet := make(map[string]struct{})
	for addr := range sendCount {
		addressSet[addr] = struct{}{}
	}
	for addr := range recvCount {
		addressSet[addr] = struct{}{}
	}
	// 写入每个地址的发送和接收次数
	for addr := range addressSet {
		total := sendCount[addr] + recvCount[addr]
		writer.Write([]string{
			addr,
			fmt.Sprintf("%d", sendCount[addr]),
			fmt.Sprintf("%d", recvCount[addr]),
			fmt.Sprintf("%d", total),
		})
	}
	t.Logf("统计完成，结果已保存到 %s", outputFile)
}
