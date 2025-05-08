package dataSupport

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"sync"
)

type Data_supportRLPA struct {
	ModifiedMap             []map[string]uint64                   // record the modified map from the decider(s)
	AccountTransferRound    uint64                                // denote how many times accountTransfer do
	ReceivedNewAccountState map[string]*core.AccountState         // the new accountState From other Shards
	ReceivedNewTx           []*core.Transaction                   // new transactions from other shards' pool
	AccountStateTx          map[uint64]*message.AccountStateAndTx // the map of accountState and transactions, pool
	PartitionOn             bool                                  // judge nextEpoch is partition or not

	PartitionReady map[uint64]bool // judge whether all shards has done all txs
	P_ReadyLock    sync.Mutex      // lock for ready

	ReadySeq     map[uint64]uint64 // record the seqid when the shard is ready
	ReadySeqLock sync.Mutex        // lock for seqMap

	CollectOver bool       // judge whether all txs is collected or not
	CollectLock sync.Mutex // lock for collect
	// TransactionTimes map[string][]int64 // 存储每对账户之间的交易时间

	// // 新增字段
	// AccountFrequency map[string]int  // 存储账户的交易频率
	// HotAccounts      map[string]bool // 存储热点账户
	// HotAccountLock   sync.Mutex      // 锁，用于保护热点账户的更新
}

func NewRLPADataSupport() *Data_supportRLPA {
	return &Data_supportRLPA{
		ModifiedMap:             make([]map[string]uint64, 0),
		AccountTransferRound:    0,
		ReceivedNewAccountState: make(map[string]*core.AccountState),
		ReceivedNewTx:           make([]*core.Transaction, 0),
		AccountStateTx:          make(map[uint64]*message.AccountStateAndTx),
		PartitionOn:             false,
		PartitionReady:          make(map[uint64]bool),
		CollectOver:             false,
		ReadySeq:                make(map[uint64]uint64),
		// TransactionTimes:        make(map[string][]int64),

		// AccountFrequency: make(map[string]int),
		// HotAccounts:      make(map[string]bool),
	}
}

// // 添加交易时间
// func (ds *Data_supportRLPA) AddTransactionTime(sender, recipient string, t_now int64) {
// 	key := sender + "->" + recipient
// 	ds.TransactionTimes[key] = append(ds.TransactionTimes[key], t_now)
// }

// // 更新账户交易频率
// func (ds *Data_supportRLPA) UpdateAccountFrequency(sender, recipient string) {
// 	ds.HotAccountLock.Lock()
// 	defer ds.HotAccountLock.Unlock()

// 	ds.AccountFrequency[sender]++
// 	ds.AccountFrequency[recipient]++

// 	// 判断是否为热点账户
// 	if ds.AccountFrequency[sender] > 1000 {
// 		ds.HotAccounts[sender] = true
// 	}
// 	if ds.AccountFrequency[recipient] > 1000 {
// 		ds.HotAccounts[recipient] = true
// 	}
// }

// // 判断是否为热点账户
// func (ds *Data_supportRLPA) IsHotAccount(account string) bool {
// 	ds.HotAccountLock.Lock()
// 	defer ds.HotAccountLock.Unlock()

// 	return ds.HotAccounts[account]
// }
