package controllers

import "strconv"

type ChainRequest struct {
	Chain []Block `json:chain`
	NewTx StandTx `json:"new_tx"`
}

type Block struct {
	BlockId uint64 `json:id`
	BlcokHash string `json:hash`
	Pow string `json:pow`
	AllTx []StandTx `json:"all_tx"`
}

type StandTx struct {
	Send uint64 `json:"send"`
	Recv uint64 `json:"recv"`
	Amt uint64 `json:"amt"`
	Fee uint64 `json:"fee"`
	TxHash string `json:"hash"`
	Time string `json:"time"`
	Sig uint64 `json:"sig"`
}






func checkBlcokChain(blockChain *ChainRequest) bool {
	firstPrevBlockHash := "00000000"
	for i := 0; i < len(blockChain.Chain); i++ {
		// check the transactions in the block first
		for j := 0; j < len(blockChain.Chain[i].AllTx); j++ {
			if !checkTx(blockChain.Chain[i].AllTx[j]) {
				return false
			}
		}
		// check the block hash
		if i == 0 {
			if !checkBlock(blockChain.Chain[i], firstPrevBlockHash){
				return false
			}
		} else {
			if !checkBlock(blockChain.Chain[i], blockChain.Chain[i-1].BlcokHash){
				return false
			}
		}
	}
	return true
}

func checkBlock(block Block, prevBlockHash string) bool{
	blockStr := strconv.FormatUint(block.BlockId, 10) + "|" + prevBlockHash
	for i := 0; i < len(block.AllTx); i++ {
		blockStr += "|"
		blockStr += block.AllTx[i].TxHash

	}
	if CCHashBlock(blockStr, block.Pow) == block.BlcokHash {
		return true
	} else {
		return false
	}
}

func checkTx(tx StandTx) bool{
	// check the reward fee
	if tx.Send == uint64(0) && tx.Sig == uint64(0) && tx.Fee == uint64(0) {
		if tx.Fee > uint64(500000000) {
			return false
		}
	}
	txStr := tx.Time + "|"
	if tx.Send != uint64(0) {
		txStr += strconv.FormatUint(tx.Send, 10)
	}
	txStr += "|"
	if tx.Recv != uint64(0) {
		txStr += strconv.FormatUint(tx.Recv, 10)
	}
	txStr += "|"
	if tx.Amt != uint64(0) {
		txStr += strconv.FormatUint(tx.Amt, 10)
	}
	txStr += "|"
	if tx.Fee != uint64(0) {
		txStr += strconv.FormatUint(tx.Fee, 10)
	}
	if CCHash(txStr) == tx.TxHash {
		return true
	} else {
		return false
	}
}


//func generateRewardTx()

//func mining(blockChain *ChainRequest) string {
//	var preBlockHash string
//	var newBlock Block
//	newBlock.BlockId = uint64(len(blockChain.Chain))
//	append(newBlock.AllTx, blockChain.NewTx)
//	if newBlock.BlockId == 0 {
//		preBlockHash = "00000000"
//	} else {
//		preBlockHash = blockChain.Chain[newBlock.BlockId-1].BlcokHash
//	}
//
//
//}

//func subMiner() {
//
//}