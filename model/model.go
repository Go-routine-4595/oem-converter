package model

import "time"

type Item struct {
	Rcv  time.Time
	Data []byte
}
