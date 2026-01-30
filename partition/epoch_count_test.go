package partition

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestPeerAccountCountInEpochFiles(t *testing.T) {
	// 匹配所有 account_pair_epoch_*.csv 文件
	files, err := filepath.Glob("account_pair_epoch_*.csv")
	if err != nil {
		t.Fatalf("查找文件失败: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("未找到任何 account_pair_epoch_*.csv 文件")
	}

	for _, file := range files {
		peerCount := make(map[string]int)
		f, err := os.Open(file)
		if err != nil {
			t.Errorf("无法打开文件 %s: %v", file, err)
			continue
		}
		reader := csv.NewReader(f)
		_, _ = reader.Read() // 跳过表头
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil || len(record) < 2 {
				continue
			}
			peer := record[1] // B列
			peerCount[peer]++
		}
		f.Close()

		// 输出统计结果到新文件
		outfile := file[:len(file)-4] + "_peer_count.csv"
		out, err := os.Create(outfile)
		if err != nil {
			t.Errorf("无法创建输出文件 %s: %v", outfile, err)
			continue
		}
		writer := csv.NewWriter(out)
		defer writer.Flush()
		writer.Write([]string{"PeerAccount", "Count"})
		for peer, count := range peerCount {
			writer.Write([]string{peer, fmt.Sprintf("%d", count)})
		}
		out.Close()
		t.Logf("统计完成，结果已保存到 %s", outfile)
	}
}
