package adapter

import "time"

type CreateMovieRequest struct {
	MovieID int64  `json:"movie_id"`
	Title   string `json:"title"`
	Action  string `json:"action"`
	UserID  int64  `json:"user_id"`
}

type CreateUserRequest struct {
	UserID    int64     `json:"user_id"`
	Username  string    `json:"username"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type CreatePaymentRequest struct {
	PaymentID  int64     `json:"payment_id"`
	UserID     int64     `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type"`
}
