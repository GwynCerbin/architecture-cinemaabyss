package entities

import "time"

type Payment struct {
	PaymentID  int64
	UserID     int64
	Amount     float64
	Status     string
	Timestamp  time.Time
	MethodType string
}
