package main

import (
	"context"
	"log"
	"time"

	"events/internal/controller/httpserver"
	"events/internal/controller/httpserver/handlers/event"
	"events/internal/controller/httpserver/handlers/health"
	"events/internal/controller/listener"
	kafkaevent "events/internal/controller/listener/handlers/event"
	"events/internal/domain/usecase"
	"events/internal/infra/eventProvider"
	"events/pkg/app"
	"events/pkg/configrenderer"
	"events/pkg/kafkaclient"
	kafkatopics "events/pkg/kafkalistener"

	"github.com/gin-gonic/gin"
)

func main() {
	config := Config{Worker: kafkatopics.DefaultConfig()}
	if err := configrenderer.Render(&config); err != nil {
		log.Printf("failed to render config: %v", err)
		return
	}
	application := app.New(10 * time.Second)
	defer func() {
		if err := application.Close(); err != nil {
			log.Printf("application cleanup: %v", err)
		}
	}()

	brokerClient, err := kafkaclient.NewClient(config.Kafka)
	if err != nil {
		log.Printf("init kafka client: %v", err)
		return
	}
	if err := application.AddCleanup("kafka publisher", func(context.Context) error { return brokerClient.Close() }); err != nil {
		_ = brokerClient.Close()
		log.Printf("register kafka publisher cleanup: %v", err)
		return
	}

	publisher, err := eventProvider.NewPublisher(brokerClient, config.Publisher)
	if err != nil {
		log.Printf("init event publisher: %v", err)
		return
	}

	consumer, err := kafkaclient.NewConsumer(config.consumerConfig())
	if err != nil {
		log.Printf("init kafka consumer: %v", err)
		return
	}
	if err := application.AddCleanup("kafka consumer", func(context.Context) error { return consumer.Close() }); err != nil {
		_ = consumer.Close()
		log.Printf("register kafka consumer cleanup: %v", err)
		return
	}

	if err := application.AddController("http listener", app.Controller{
		Run: func(ctx context.Context) error {
			router := gin.Default()
			group := router.Group("/api/events")

			eventProcessingUC := usecase.NewUseCase(publisher)

			health.NewHealthHandler(group)
			event.NewHealthHandlerSet(group, eventProcessingUC)
			return httpserver.NewHTTPServer(router, config.HTTPServer).ListenAndServe(ctx)
		},
	}); err != nil {
		log.Printf("failed to add HTTP controller: %v", err)
		return
	}

	if err := application.AddController("kafka listener", app.Controller{
		Run: func(ctx context.Context) error {
			router := listener.NewRouter()
			kafkaevent.NewHandlerSet(router, kafkaevent.Config{
				MovieTopic:   config.Publisher.MovieTopic,
				UserTopic:    config.Publisher.UserTopic,
				PaymentTopic: config.Publisher.PaymentTopic,
			})
			worker, err := kafkatopics.NewListener(ctx, consumer, router, kafkatopics.WithConfig(config.Worker))
			if err != nil {
				return err
			}
			return listener.NewServer(worker, config.Listener).ListenAndServe(ctx)
		},
	}); err != nil {
		log.Printf("failed to add Kafka controller: %v", err)
		return
	}

	if err := application.Run(); err != nil {
		log.Printf("application runtime error: %v", err)
	}
}
