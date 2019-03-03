package controllers

import (
	"fmt"
	"testing"
)

func TestCCHash(t *testing.T) {
	res := CCHash("1552533255926535897|15213|15619|528491|24601")
	fmt.Println(res)
	//fmt.Printf("%x", []byte(res))
}

func TestCheckTx(t *testing.T) {
	tx := StandTx{
		Send:   1284110893049,
		Recv:   484054352161,
		Amt:    58759591,
		Fee:    5048,
		Time:   "1550721967779447040",
		TxHash: "b43737af",
		Sig:    1084970046728}
	rewadrTx := StandTx{
		Recv:   34123506233,
		Amt:    5000000001,
		Time:   "1550721967779474176",
		TxHash: "d705e74e"}
	fmt.Println(checkTx(tx))
	fmt.Println(checkTx(rewadrTx))
}

func TestCheckBlock(t *testing.T) {
	block := Block{
		BlockId:   1,
		BlcokHash: "0fce51c1",
		Pow:       "fountain",
		AllTx: []StandTx{
			{
				Send:   509015179679,
				Recv:   484054352161,
				Amt:    126848946,
				Fee:    12488,
				Time:   "1550721967779391744",
				TxHash: "5a2b4d71",
				Sig:    463884077351,
			},
			{
				Recv:   1284110893049,
				Amt:    500000000,
				Time:   "1550721967779424000",
				TxHash: "7924c55e",
			},
		},
	}
	fmt.Println(checkBlock(block, "02899b89"))
}
