package adapter

import "events/internal/domain/entities"

func (r CreateMovieRequest) ToEntity() *entities.Movie {
	return &entities.Movie{
		MovieID: r.MovieID,
		Title:   r.Title,
		Action:  r.Action,
		UserID:  r.UserID,
	}
}

func (r CreateUserRequest) ToEntity() *entities.User {
	return &entities.User{
		UserID:    r.UserID,
		Username:  r.Username,
		Action:    r.Action,
		Timestamp: r.Timestamp,
	}
}

func (r CreatePaymentRequest) ToEntity() *entities.Payment {
	return &entities.Payment{
		PaymentID:  r.PaymentID,
		UserID:     r.UserID,
		Amount:     r.Amount,
		Status:     r.Status,
		Timestamp:  r.Timestamp,
		MethodType: r.MethodType,
	}
}
