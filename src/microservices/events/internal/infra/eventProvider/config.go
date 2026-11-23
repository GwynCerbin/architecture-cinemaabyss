package eventProvider

type Config struct {
	MovieTopic   string `env:"MOVIE_TOPIC" envDefault:"movie-events"`
	UserTopic    string `env:"USER_TOPIC" envDefault:"user-events"`
	PaymentTopic string `env:"PAYMENT_TOPIC" envDefault:"payment-events"`
}

func DefaultConfig() Config {
	return Config{MovieTopic: "movie-events", UserTopic: "user-events", PaymentTopic: "payment-events"}
}
