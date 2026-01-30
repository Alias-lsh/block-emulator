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

func TestAddressPairTransactionCount(t *testing.T) {
	const (
		outputFile = "address_pair_transaction_count.csv"
		fromCol    = 3 // 发送方地址列索引
		toCol      = 4 // 接收方地址列索引
		maxLines   = 3000000
	)
	inputFile := "../" + params.DatasetFile

	// 统计每个账户与哪些账户交易过以及次数
	// map[账户]map[对方账户]交易次数
	accountPairCount := make(map[string]map[string]int)

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
		if fromAddr == "" || toAddr == "" {
			continue
		}
		// 统计发送方与接收方的交易
		if accountPairCount[fromAddr] == nil {
			accountPairCount[fromAddr] = make(map[string]int)
		}
		accountPairCount[fromAddr][toAddr]++
	}

	// 统计每个账户的组内总交易量
	type accountStat struct {
		Account      string
		TotalTxCount int
	}
	var accountStats []accountStat
	for account, peers := range accountPairCount {
		total := 0
		for _, count := range peers {
			total += count
		}
		accountStats = append(accountStats, accountStat{account, total})
	}
	// 按组内总交易量降序排序账户
	sort.Slice(accountStats, func(i, j int) bool {
		return accountStats[i].TotalTxCount > accountStats[j].TotalTxCount
	})

	// 写入结果到CSV
	out, err := os.Create(outputFile)
	if err != nil {
		t.Fatalf("无法创建输出文件: %v", err)
	}
	defer out.Close()
	writer := csv.NewWriter(out)
	defer writer.Flush()
	// 标题
	if err := writer.Write([]string{"Account", "PeerAccount", "TxCount"}); err != nil {
		t.Fatalf("写入标题失败: %v", err)
	}
	// 先按账户组内总交易量降序输出
	for _, acc := range accountStats {
		account := acc.Account
		peers := accountPairCount[account]
		// 组内按交易量降序
		type peerStat struct {
			Peer  string
			Count int
		}
		var peerList []peerStat
		for peer, count := range peers {
			peerList = append(peerList, peerStat{peer, count})
		}
		sort.Slice(peerList, func(i, j int) bool {
			return peerList[i].Count > peerList[j].Count
		})
		for _, item := range peerList {
			writer.Write([]string{
				account,
				item.Peer,
				fmt.Sprintf("%d", item.Count),
			})
		}
	}
	t.Logf("统计完成，结果已保存到 %s", outputFile)
}
