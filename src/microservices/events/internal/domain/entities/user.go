package entities

import "time"

type User struct {
	UserID    int64
	Username  string
	Action    string
	Timestamp time.Time
}
