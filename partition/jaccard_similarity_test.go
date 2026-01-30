package partition

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// 读取每个epoch的账户-peer集合
func readEpochPeers(filename string) (map[string]map[string]struct{}, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	_, _ = reader.Read() // 跳过表头
	accountPeers := make(map[string]map[string]struct{})
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(record) < 2 {
			continue
		}
		account := record[0]
		peer := record[1]
		if accountPeers[account] == nil {
			accountPeers[account] = make(map[string]struct{})
		}
		accountPeers[account][peer] = struct{}{}
	}
	return accountPeers, nil
}

func TestAccountPeerJaccardSimilarity(t *testing.T) {
	// 假设你有 account_pair_epoch_0.csv, account_pair_epoch_1.csv, ...
	epochFiles := []string{}
	// 自动查找所有epoch文件
	files, _ := filepath.Glob("account_pair_epoch_*.csv")
	epochFiles = append(epochFiles, files...)
	if len(epochFiles) < 2 {
		t.Fatalf("需要至少两个epoch的csv文件")
	}

	// 读取所有epoch的账户-peer集合
	epochPeers := make([]map[string]map[string]struct{}, len(epochFiles))
	for i, file := range epochFiles {
		peers, err := readEpochPeers(file)
		if err != nil {
			t.Fatalf("读取%s失败: %v", file, err)
		}
		epochPeers[i] = peers
	}

	// 计算每个账户在相邻epoch的peer集合Jaccard相似度
	out, err := os.Create("account_peer_jaccard_similarity.csv")
	if err != nil {
		t.Fatalf("无法创建输出文件: %v", err)
	}
	defer out.Close()
	writer := csv.NewWriter(out)
	defer writer.Flush()
	writer.Write([]string{"Epoch", "Account", "JaccardSimilarity"})

	for i := 0; i < len(epochPeers)-1; i++ {
		peersNow := epochPeers[i]
		peersNext := epochPeers[i+1]
		// 统计所有账户
		accountSet := make(map[string]struct{})
		for acc := range peersNow {
			accountSet[acc] = struct{}{}
		}
		for acc := range peersNext {
			accountSet[acc] = struct{}{}
		}
		for acc := range accountSet {
			set1 := peersNow[acc]
			set2 := peersNext[acc]
			// 计算Jaccard
			inter, union := 0, 0
			unionSet := make(map[string]struct{})
			for peer := range set1 {
				unionSet[peer] = struct{}{}
			}
			for peer := range set2 {
				unionSet[peer] = struct{}{}
			}
			for peer := range set1 {
				if _, ok := set2[peer]; ok {
					inter++
				}
			}
			union = len(unionSet)
			var jac float64
			if union > 0 {
				jac = float64(inter) / float64(union)
			} else {
				jac = 1 // 两个都空视为完全相似
			}
			writer.Write([]string{
				fmt.Sprintf("%d-%d", i, i+1),
				acc,
				fmt.Sprintf("%.6f", jac),
			})
		}
	}
	t.Log("账户peer相似度统计完成，结果已保存到 account_peer_jaccard_similarity.csv")
}
