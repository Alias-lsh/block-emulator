package partition

import (
	"blockEmulator/params"
	"encoding/csv"
	"io"
	"log"
	"os"
	"testing"
	"time"
)

// const TimeWindow float64 = 60

func TestRlpa(t *testing.T) {
	// 初始化 RLPAState
	k := new(RLPAState)
	k.Init_RLPAState(0.5, 100, 4) // 权重惩罚 0.5，最大迭代次数 100，分片数 4
	hotAccountThreshold := 1000

	// 打开交易数据文件
	txfile, err := os.Open("../" + params.DatasetFile)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()

	reader := csv.NewReader(txfile)
	datanum := 0

	// 账户交易频率统计
	accountFrequency := make(map[string]int)

	// 读取交易数据
	reader.Read() // 跳过表头
	for {
		data, err := reader.Read()
		if err == io.EOF || datanum == 200000 { // 限制读取 200,000 条数据
			break
		}
		if err != nil {
			log.Panic(err)
		}

		// 过滤有效交易并添加到图中
		if data[6] == "0" && data[7] == "0" && len(data[3]) > 16 && len(data[4]) > 16 && data[3] != data[4] {
			sender := data[3][2:]
			recipient := data[4][2:] // 更新账户交易频率
			accountFrequency[sender]++
			accountFrequency[recipient]++
			// 检查是否为热点账户
			isSenderHot := accountFrequency[sender] > hotAccountThreshold
			isRecipientHot := accountFrequency[recipient] > hotAccountThreshold

			if isSenderHot || isRecipientHot {
				s := Vertex{
					Addr: sender, // 去掉前缀
				}
				r := Vertex{
					Addr: recipient, // 去掉前缀
				}
				t_now := time.Now().Unix()
				// k.AddEdge(s, r, 1) // 默认边权重为 1
				k.AddEdgeWithTime2(s, r, float64(t_now))
			}
			datanum++
		}
	}
	// // 打印热点账户信息
	// log.Println("Hot Accounts:")
	// for account, freq := range accountFrequency {
	// 	if freq > hotAccountThreshold {
	// 		log.Printf("Account: %s, Frequency: %d\n", account, freq)
	// 	}
	// }

	// 执行 RLPA 分片算法
	k.RLPA_Partition()
}
