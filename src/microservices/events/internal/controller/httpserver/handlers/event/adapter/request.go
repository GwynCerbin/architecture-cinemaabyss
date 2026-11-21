package adapter

import "time"

type CreateMovieRequest struct {
	MovieID int64  `json:"movie_id" binding:"required,gte=1"`
	Title   string `json:"title" binding:"required,min=1,max=255"`
	Action  string `json:"action" binding:"required,min=1,max=255"`
	UserID  int64  `json:"user_id" binding:"required,gte=1"`
}

type CreateUserRequest struct {
	UserID    int64     `json:"user_id" binding:"required,gte=1"`
	Username  string    `json:"username" binding:"required"`
	Action    string    `json:"action" binding:"required"`
	Timestamp time.Time `json:"timestamp" binding:"required"`
}

type CreatePaymentRequest struct {
	PaymentID  int64     `json:"payment_id" binding:"required,gte=1"`
	UserID     int64     `json:"user_id" binding:"required,gte=1"`
	Amount     float64   `json:"amount" binding:"required,gt=0"`
	Status     string    `json:"status" binding:"required"`
	Timestamp  time.Time `json:"timestamp" binding:"required"`
	MethodType string    `json:"method_type" binding:"required"`
}
