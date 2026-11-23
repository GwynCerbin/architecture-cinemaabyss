package adapters

import (
	"time"

	"events/internal/domain/entities"
)

type MovieEvent struct {
	MovieID int64  `json:"movie_id"`
	Title   string `json:"title"`
	Action  string `json:"action"`
	UserID  int64  `json:"user_id"`
}

func FromMovie(event *entities.Movie) MovieEvent {
	return MovieEvent{
		MovieID: event.MovieID,
		Title:   event.Title,
		Action:  event.Action,
		UserID:  event.UserID,
	}
}

type PaymentEvent struct {
	PaymentID  int64     `json:"payment_id"`
	UserID     int64     `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type"`
}

func FromPayment(event *entities.Payment) PaymentEvent {
	return PaymentEvent{
		PaymentID:  event.PaymentID,
		UserID:     event.UserID,
		Amount:     event.Amount,
		Status:     event.Status,
		Timestamp:  event.Timestamp,
		MethodType: event.MethodType,
	}
}

type UserEvent struct {
	UserID    int64     `json:"user_id"`
	Username  string    `json:"username"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

func FromUser(event *entities.User) UserEvent {
	return UserEvent{
		UserID:    event.UserID,
		Username:  event.Username,
		Action:    event.Action,
		Timestamp: event.Timestamp,
	}
}
