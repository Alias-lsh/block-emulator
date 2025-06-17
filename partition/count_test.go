package partition

import (
	"blockEmulator/params"
	"encoding/csv"
	"io"
	"os"
	"testing"
)

func TestCountCSVLines(t *testing.T) {
	inputFile := "../" + params.DatasetFile // 替换为你的CSV文件路径

	file, err := os.Open(inputFile)
	if err != nil {
		t.Fatalf("无法打开文件: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	lineCount := 0
	for {
		_, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("读取CSV出错: %v", err)
		}
		lineCount++
	}
	t.Logf("CSV文件总行数: %d", lineCount)
}
