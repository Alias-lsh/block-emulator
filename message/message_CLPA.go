package message

import (
	"blockEmulator/core"
	"bytes"
	"encoding/gob"
	"log"
)

var (
	AccountState_and_TX MessageType = "AccountState&txs"
	PartitionReq        RequestType = "PartitionReq"
	CPartitionMsg       MessageType = "PartitionModifiedMap"
	CPartitionReady     MessageType = "ready for partition"
)

type PartitionModifiedMap struct {
	PartitionModified map[string]uint64
}

type AccountTransferMsg struct {
	ModifiedMap  map[string]uint64
	Addrs        []string
	AccountState []*core.AccountState
	ATid         uint64
	Sender       string `json:"sender"`    // 发送方地址
	Recipient    string `json:"recipient"` // 接收方地址
	Amount       int    `json:"amount"`    // 转账金额
	Timestamp    int64  `json:"timestamp"` // 转账时间
}

type PartitionReady struct {
	FromShard uint64
	NowSeqID  uint64
}

// this message used in inter-shard, it will be sent between leaders.
type AccountStateAndTx struct {
	Addrs        []string
	AccountState []*core.AccountState
	Txs          []*core.Transaction
	FromShard    uint64
}

func (atm *AccountTransferMsg) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(atm)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeAccountTransferMsg(content []byte) *AccountTransferMsg {
	var atm AccountTransferMsg

	decoder := gob.NewDecoder(bytes.NewReader(content))
	err := decoder.Decode(&atm)
	if err != nil {
		log.Panic(err)
	}

	return &atm
}
