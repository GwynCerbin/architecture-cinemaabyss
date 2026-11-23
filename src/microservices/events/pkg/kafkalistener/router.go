package kafkatopics

import (
	"errors"
	"time"
)

// Handler configures one element of a sequential topic chain.
type Handler struct {
	handler ProcessEvent
	timeout time.Duration
}

type HandlerOption func(*Handler)

func NewHandler(handler ProcessEvent, opts ...HandlerOption) Handler {
	set := Handler{handler: handler}
	for _, opt := range opts {
		opt(&set)
	}

	return set
}

func WithTimeout(timeout time.Duration) HandlerOption {
	return func(set *Handler) { set.timeout = timeout }
}

type RouteOption func(*HandlerSet)

// HandlerSet holds the handler chain and options for one topic route.
type HandlerSet struct {
	handlers   []Handler
	options    []Option
	deadLetter bool
}

func NewHandlerSet(handlers []Handler, opts ...RouteOption) HandlerSet {
	set := HandlerSet{handlers: append([]Handler(nil), handlers...)}
	for _, opt := range opts {
		opt(&set)
	}

	return set
}

// Router is configured before starting the listener. The listener snapshots it.
type Router map[string]HandlerSet

func NewRouter() Router { return make(Router) }

// Add registers one handler with options for its topic route.
func (r Router) Add(topic string, handler ProcessEvent, opts ...RouteOption) {
	r.AddChain(topic, []Handler{NewHandler(handler)}, opts...)
}

// AddChain replaces the topic route. All handlers run, including after errors.
// Route options override listener defaults only for this topic's reader.
func (r Router) AddChain(topic string, handlers []Handler, opts ...RouteOption) {
	r[topic] = NewHandlerSet(handlers, opts...)
}

// WithDeadLetter appends one DLQ handler after the route's business handlers,
// before acknowledgement. It does not enable DLQ on other routes.
func WithDeadLetter() RouteOption { return func(set *HandlerSet) { set.deadLetter = true } }

// WithRouteOptions overrides listener dependencies only for this route's reader.
// Supplying a writer does not enable DLQ handlers.
func WithRouteOptions(opts ...Option) RouteOption {
	return func(set *HandlerSet) { set.options = append(set.options, opts...) }
}

type handlerResult struct {
	disposition Disposition
	err         error
}

type renderedChain func(*Context, int) handlerResult

func renderTopic(name string, chain HandlerSet, config listenerConfig) renderedChain {
	handlers := make([]ProcessEvent, 0, len(chain.handlers)+2)

	for _, set := range chain.handlers {
		handlers = append(handlers, renderHandler(set))
	}

	if chain.deadLetter {
		handlers = append(handlers, renderHandler(NewHandler(NewDeadLetterHandler(config.deadLetterWriter).Handle)))
	}

	handlers = append(handlers, finalHandler())

	return func(source *Context, attempt int) handlerResult {
		delivery := source
		businessCtx := source.Context
		delivery.attempt = attempt
		delivery.fatal = nil
		delivery.disposition = Pending

		attemptHandlers := handlers

		if delivery.processingErr != nil {
			delivery.Reject()

			attemptHandlers = handlers[len(chain.handlers):]
		}

		for _, handler := range attemptHandlers {
			delivery.Context = businessCtx

			delivery.processingErr = errors.Join(delivery.processingErr, handler(delivery))
		}

		return handlerResult{disposition: delivery.Disposition(), err: delivery.fatal}
	}
}
