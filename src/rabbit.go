// Package rabbit provides a comprehensive RabbitMQ client with connection pooling,
// circuit breaker, and cluster support for Go applications.
//
// This package offers enterprise-grade features including:
// - Connection pooling and channel management
// - Circuit breaker pattern for fault tolerance
// - Cluster failover support with multiple strategies
// - Exponential backoff for reconnection attempts
// - Health monitoring and metrics collection
// - Graceful shutdown with in-flight message handling
//
// Author: SID
// Version: 0.0.1
// Since: 2025-05-26
package rabbit

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

// Constants for validation and configuration
const (
	// MinHeartbeat is the minimum heartbeat interval in seconds
	MinHeartbeat = 1
	// MaxHeartbeat is the maximum heartbeat interval in seconds
	MaxHeartbeat = 60
	// MinReconnectDelay is the minimum reconnect delay in milliseconds
	MinReconnectDelay = 1000
	// MaxReconnectDelay is the maximum reconnect delay in milliseconds
	MaxReconnectDelay = 60000
	// DefaultChannelCheckInterval is the default channel availability check interval
	DefaultChannelCheckInterval = 100 * time.Millisecond
	// DefaultMetricsInterval is the default metrics emission interval
	DefaultMetricsInterval = 60 * time.Second
	// MaximumInitialConnectionRetries is the maximum initial connection retry attempts
	MaximumInitialConnectionRetries = 5
)

// FailoverStrategy defines the strategy for selecting cluster nodes
type FailoverStrategy string

const (
	// RoundRobin cycles through nodes in order
	RoundRobin FailoverStrategy = "round-robin"
	// Random selects nodes randomly
	Random FailoverStrategy = "random"
)

// Metrics represents performance and operational metrics
type Metrics struct {
	// MessagesSent is the total number of messages sent
	MessagesSent int64 `json:"messagesSent"`
	// MessagesReceived is the total number of messages received
	MessagesReceived int64 `json:"messagesReceived"`
	// Errors is the total number of errors encountered
	Errors int64 `json:"errors"`
	// Reconnections is the total number of reconnection attempts
	Reconnections int64 `json:"reconnections"`
	// LastReconnectTime is the timestamp of last reconnection
	LastReconnectTime *time.Time `json:"lastReconnectTime"`
	// AvgProcessingTime is the average message processing time in milliseconds
	AvgProcessingTime float64 `json:"avgProcessingTime"`
}

// CircuitBreakerConfig defines circuit breaker configuration
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of failures before opening circuit
	FailureThreshold int `json:"failureThreshold"`
	// ResetTimeout is the time in milliseconds before attempting to close circuit
	ResetTimeout time.Duration `json:"resetTimeout"`
}

// PoolConfig defines channel pool configuration
type PoolConfig struct {
	// MaxChannels is the maximum number of channels in pool
	MaxChannels int `json:"maxChannels"`
	// AcquireTimeout is the timeout for acquiring channel from pool
	AcquireTimeout time.Duration `json:"acquireTimeout"`
}

// BatchConfig defines message batching configuration
type BatchConfig struct {
	// Size is the maximum messages per batch
	Size int `json:"size"`
	// TimeoutMs is the maximum time to wait before sending batch
	TimeoutMs time.Duration `json:"timeoutMs"`
}

// SSLConfig defines SSL/TLS configuration
type SSLConfig struct {
	// Enabled indicates whether SSL is enabled
	Enabled bool `json:"enabled"`
	// Validate indicates whether to validate server certificate
	Validate bool `json:"validate"`
	// CA contains certificate authority certificates
	CA []string `json:"ca"`
	// Cert is the client certificate
	Cert string `json:"cert"`
	// Key is the client private key
	Key string `json:"key"`
	// Passphrase is the private key passphrase
	Passphrase string `json:"passphrase"`
}

// ClusterOptions defines cluster-specific options
type ClusterOptions struct {
	// RetryConnectTimeout is the time to wait before trying next node
	RetryConnectTimeout time.Duration `json:"retryConnectTimeout"`
	// NodeRecoveryInterval is the interval for node health checks
	NodeRecoveryInterval time.Duration `json:"nodeRecoveryInterval"`
	// ShuffleNodes indicates whether to randomly shuffle nodes on startup
	ShuffleNodes bool `json:"shuffleNodes"`
	// PriorityNodes contains preferred nodes to try first
	PriorityNodes []string `json:"priorityNodes"`
}

// ChannelOptions defines channel recovery options
type ChannelOptions struct {
	// MaxRetries is the maximum channel recovery retries
	MaxRetries int `json:"maxRetries"`
	// RetryDelay is the delay between channel recovery attempts
	RetryDelay time.Duration `json:"retryDelay"`
	// AutoRecovery indicates whether to automatically recover channels
	AutoRecovery bool `json:"autoRecovery"`
}

// QueueOptions defines queue assertion options with additional RabbitMQ features
type QueueOptions struct {
	// Durable indicates if queue survives server restart
	Durable bool `json:"durable"`
	// AutoDelete indicates if queue is deleted when unused
	AutoDelete bool `json:"autoDelete"`
	// Exclusive indicates if queue is exclusive to connection
	Exclusive bool `json:"exclusive"`
	// NoWait indicates if server should not wait for confirmation
	NoWait bool `json:"noWait"`
	// Args contains additional queue arguments
	Args amqp.Table `json:"args"`
	// DeadLetterExchange is the dead letter exchange name
	DeadLetterExchange string `json:"deadLetterExchange"`
	// DeadLetterRoutingKey is the dead letter routing key
	DeadLetterRoutingKey string `json:"deadLetterRoutingKey"`
	// MessageTTL is the message time-to-live in milliseconds
	MessageTTL int32 `json:"messageTtl"`
	// Expires is the queue expiration time in milliseconds
	Expires int32 `json:"expires"`
	// MaxLength is the maximum queue length
	MaxLength int32 `json:"maxLength"`
	// MaxPriority is the maximum message priority
	MaxPriority uint8 `json:"maxPriority"`
}

// ExchangeOptions defines exchange assertion options with additional RabbitMQ features
type ExchangeOptions struct {
	// Kind is the exchange type (direct, topic, fanout, headers)
	Kind string `json:"kind"`
	// Durable indicates if exchange survives server restart
	Durable bool `json:"durable"`
	// AutoDelete indicates if exchange is deleted when unused
	AutoDelete bool `json:"autoDelete"`
	// Internal indicates if exchange is internal
	Internal bool `json:"internal"`
	// NoWait indicates if server should not wait for confirmation
	NoWait bool `json:"noWait"`
	// Args contains additional exchange arguments
	Args amqp.Table `json:"args"`
	// AlternateExchange is the alternate exchange for unroutable messages
	AlternateExchange string `json:"alternateExchange"`
}

// PublishOptions defines message publishing options
type PublishOptions struct {
	// Exchange is the target exchange name
	Exchange string `json:"exchange"`
	// RoutingKey is the message routing key
	RoutingKey string `json:"routingKey"`
	// Mandatory indicates if message must be routable
	Mandatory bool `json:"mandatory"`
	// Immediate indicates if message must be immediately consumable
	Immediate bool `json:"immediate"`
	// Publishing contains AMQP publishing properties
	Publishing amqp.Publishing `json:"publishing"`
	// Timeout is the publish operation timeout
	Timeout time.Duration `json:"timeout"`
}

// ConsumeOptions defines message consumption options
type ConsumeOptions struct {
	// Consumer is the consumer identifier
	Consumer string `json:"consumer"`
	// AutoAck indicates if messages are automatically acknowledged
	AutoAck bool `json:"autoAck"`
	// Exclusive indicates if consumer is exclusive
	Exclusive bool `json:"exclusive"`
	// NoLocal indicates if messages published on same connection are not delivered
	NoLocal bool `json:"noLocal"`
	// NoWait indicates if server should not wait for confirmation
	NoWait bool `json:"noWait"`
	// Args contains additional consumer arguments
	Args amqp.Table `json:"args"`
	// Timeout is the message processing timeout
	Timeout time.Duration `json:"timeout"`
}

// Config represents comprehensive RabbitMQ client configuration options
type Config struct {
	// URLs is the array of cluster node URLs
	URLs []string `json:"urls"`
	// Heartbeat is the heartbeat interval in seconds
	Heartbeat time.Duration `json:"heartbeat"`
	// ConnectionName is the human-readable connection name for debugging
	ConnectionName string `json:"connectionName"`
	// PrefetchCount is the per-channel message prefetch count
	PrefetchCount int `json:"prefetchCount"`
	// PrefetchGlobal indicates whether prefetch applies globally
	PrefetchGlobal bool `json:"prefetchGlobal"`
	// ReconnectDelay is the delay between reconnection attempts
	ReconnectDelay time.Duration `json:"reconnectDelay"`
	// MaxReconnectAttempts is the maximum number of reconnection attempts (-1 for unlimited)
	MaxReconnectAttempts int `json:"maxReconnectAttempts"`
	// ExponentialBackoff indicates whether to use exponential backoff for reconnection delays
	ExponentialBackoff bool `json:"exponentialBackoff"`
	// PoolConfig is the channel pool configuration
	PoolConfig PoolConfig `json:"poolConfig"`
	// CircuitBreaker is the circuit breaker configuration
	CircuitBreaker CircuitBreakerConfig `json:"circuitBreaker"`
	// BatchConfig is the message batching configuration
	BatchConfig BatchConfig `json:"batchConfig"`
	// ConnectionTimeout is the connection timeout
	ConnectionTimeout time.Duration `json:"connectionTimeout"`
	// FailoverStrategy is the strategy for selecting cluster nodes
	FailoverStrategy FailoverStrategy `json:"failoverStrategy"`
	// VHost is the virtual host name
	VHost string `json:"vhost"`
	// SSL is the SSL/TLS configuration
	SSL SSLConfig `json:"ssl"`
	// ClusterOptions contains cluster-specific options
	ClusterOptions ClusterOptions `json:"clusterOptions"`
	// ChannelOptions contains channel recovery options
	ChannelOptions ChannelOptions `json:"channelOptions"`
	// Username for authentication
	Username string `json:"username"`
	// Password for authentication
	Password string `json:"password"`
}

// NodeStatus represents the status of a cluster node
type NodeStatus struct {
	// Healthy indicates whether the node is currently healthy
	Healthy bool `json:"healthy"`
	// LastChecked is the last health check timestamp
	LastChecked time.Time `json:"lastChecked"`
	// FailureCount is the number of consecutive failures
	FailureCount int `json:"failureCount"`
}

// ChannelPool manages a pool of AMQP channels
type ChannelPool struct {
	channels   []*amqp.Channel
	inUse      map[*amqp.Channel]bool
	maxSize    int
	mu         sync.RWMutex
	acquireCh  chan struct{}
	releaseCh  chan *amqp.Channel
	closeCh    chan struct{}
	connection *amqp.Connection
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	failures    int64
	isOpen      int32
	lastFailure time.Time
	config      CircuitBreakerConfig
	mu          sync.RWMutex
}

// MessageBatch represents a batch of messages for bulk publishing
type MessageBatch struct {
	Messages []PublishOptions `json:"messages"`
	timer    *time.Timer
	mu       sync.Mutex
}

// EventType represents the type of events emitted by the client
type EventType string

const (
	// EventConnecting is emitted when starting connection attempt
	EventConnecting EventType = "connecting"
	// EventConnected is emitted when successfully connected
	EventConnected EventType = "connected"
	// EventConnectionError is emitted when connection error occurs
	EventConnectionError EventType = "connectionError"
	// EventConnectionClosed is emitted when connection is closed
	EventConnectionClosed EventType = "connectionClosed"
	// EventConnectionFailed is emitted when connection attempt fails
	EventConnectionFailed EventType = "connectionFailed"
	// EventChannelError is emitted when channel error occurs
	EventChannelError EventType = "channelError"
	// EventChannelClosed is emitted when channel is closed
	EventChannelClosed EventType = "channelClosed"
	// EventChannelDrain is emitted when channel drain event occurs
	EventChannelDrain EventType = "channelDrain"
	// EventMessageReturned is emitted when message is returned by broker
	EventMessageReturned EventType = "messageReturned"
	// EventMetrics is emitted periodically with current metrics
	EventMetrics EventType = "metrics"
	// EventReconnecting is emitted when starting reconnection attempt
	EventReconnecting EventType = "reconnecting"
	// EventReconnected is emitted when successfully reconnected
	EventReconnected EventType = "reconnected"
	// EventReconnectFailed is emitted when reconnection fails
	EventReconnectFailed EventType = "reconnectFailed"
	// EventError is emitted on general errors
	EventError EventType = "error"
	// EventClosed is emitted when client is closed
	EventClosed EventType = "closed"
	// EventBlocked is emitted when connection is blocked by broker
	EventBlocked EventType = "blocked"
	// EventUnblocked is emitted when connection is unblocked by broker
	EventUnblocked EventType = "unblocked"
)

// Event represents an event emitted by the RabbitMQ client
type Event struct {
	Type EventType   `json:"type"`
	Data interface{} `json:"data"`
	Time time.Time   `json:"time"`
}

// EventHandler is a function that handles events
type EventHandler func(event Event)

// Client represents an advanced RabbitMQ client with connection pooling,
// circuit breaker, and cluster support.
//
// The client provides enterprise-grade features including:
// - Connection pooling and channel management
// - Circuit breaker pattern for fault tolerance
// - Cluster failover support with multiple strategies
// - Exponential backoff for reconnection attempts
// - Health monitoring and metrics collection
// - Graceful shutdown with in-flight message handling
//
// Example usage:
//
//	config := &rabbit.Config{
//		URLs:     []string{"amqp://localhost:5672"},
//		Heartbeat: 60 * time.Second,
//		PoolConfig: rabbit.PoolConfig{
//			MaxChannels:    10,
//			AcquireTimeout: 5 * time.Second,
//		},
//	}
//
//	client, err := rabbit.NewClient(config)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	if err := client.Connect(context.Background()); err != nil {
//		log.Fatal(err)
//	}
//
//	// Publish a message
//	err = client.Publish(context.Background(), &rabbit.PublishOptions{
//		Exchange:   "my-exchange",
//		RoutingKey: "routing.key",
//		Publishing: amqp.Publishing{
//			Body: []byte("Hello World"),
//		},
//	})
type Client struct {
	config              *Config
	connection          *amqp.Connection
	defaultChannel      *amqp.Channel
	channelPool         *ChannelPool
	circuitBreaker      *CircuitBreaker
	metrics             *Metrics
	messageBatch        *MessageBatch
	activeNodes         map[string]*NodeStatus
	currentURLIndex     int32
	reconnecting        int32
	shutdownInProgress  int32
	reconnectAttempts   int32
	connectionPromise   chan error
	eventHandlers       map[EventType][]EventHandler
	logger              *slog.Logger
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	mu                  sync.RWMutex
}

// NewClient creates a new RabbitMQ client with the specified configuration.
//
// The client must be configured with at least one URL. Other configuration
// options have sensible defaults but can be customized as needed.
//
// Parameters:
//   - config: Configuration options for the RabbitMQ client
//
// Returns:
//   - *Client: The configured RabbitMQ client
//   - error: Error if configuration validation fails
//
// Example:
//
//	config := &rabbit.Config{
//		URLs:     []string{"amqp://localhost:5672"},
//		Heartbeat: 60 * time.Second,
//	}
//	client, err := rabbit.NewClient(config)
//	if err != nil {
//		log.Fatal(err)
//	}
func NewClient(config *Config) (*Client, error) {
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Set defaults
	if config.Heartbeat == 0 {
		config.Heartbeat = 60 * time.Second
	}
	if config.ReconnectDelay == 0 {
		config.ReconnectDelay = 5 * time.Second
	}
	if config.MaxReconnectAttempts == 0 {
		config.MaxReconnectAttempts = -1
	}
	if config.ConnectionTimeout == 0 {
		config.ConnectionTimeout = 30 * time.Second
	}
	if config.FailoverStrategy == "" {
		config.FailoverStrategy = RoundRobin
	}
	if config.PoolConfig.MaxChannels == 0 {
		config.PoolConfig.MaxChannels = 10
	}
	if config.PoolConfig.AcquireTimeout == 0 {
		config.PoolConfig.AcquireTimeout = 5 * time.Second
	}
	if config.CircuitBreaker.FailureThreshold == 0 {
		config.CircuitBreaker.FailureThreshold = 5
	}
	if config.CircuitBreaker.ResetTimeout == 0 {
		config.CircuitBreaker.ResetTimeout = 30 * time.Second
	}
	if config.BatchConfig.Size == 0 {
		config.BatchConfig.Size = 100
	}
	if config.BatchConfig.TimeoutMs == 0 {
		config.BatchConfig.TimeoutMs = 1 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &Client{
		config:         config,
		metrics:        &Metrics{},
		activeNodes:    make(map[string]*NodeStatus),
		eventHandlers:  make(map[EventType][]EventHandler),
		logger:         slog.Default(),
		ctx:            ctx,
		cancel:         cancel,
		messageBatch:   &MessageBatch{Messages: make([]PublishOptions, 0)},
	}

	client.circuitBreaker = &CircuitBreaker{
		config: config.CircuitBreaker,
	}

	client.logger.Debug("RabbitMQ client initialized successfully",
		"heartbeat", config.Heartbeat,
		"maxChannels", config.PoolConfig.MaxChannels,
		"urls", len(config.URLs))

	client.initializeMetricsCollection()
	client.startNodeHealthCheck()

	return client, nil
}

// validateConfig validates the provided configuration options.
//
// Parameters:
//   - config: Configuration to validate
//
// Returns:
//   - error: Error if configuration is invalid
func validateConfig(config *Config) error {
	if config == nil {
		return errors.New("configuration cannot be nil")
	}

	if len(config.URLs) == 0 {
		return errors.New("at least one URL must be provided")
	}

	if config.Heartbeat > 0 {
		if config.Heartbeat < MinHeartbeat*time.Second || config.Heartbeat > MaxHeartbeat*time.Second {
			return fmt.Errorf("heartbeat must be between %d and %d seconds", MinHeartbeat, MaxHeartbeat)
		}
	}

	if config.ReconnectDelay > 0 {
		if config.ReconnectDelay < MinReconnectDelay*time.Millisecond || config.ReconnectDelay > MaxReconnectDelay*time.Millisecond {
			return fmt.Errorf("reconnect delay must be between %d and %d ms", MinReconnectDelay, MaxReconnectDelay)
		}
	}

	if config.PoolConfig.MaxChannels < 1 {
		return errors.New("max channels must be greater than 0")
	}

	return nil
}

// SetLogger sets a custom logger for the client.
//
// Parameters:
//   - logger: The slog.Logger instance to use
func (c *Client) SetLogger(logger *slog.Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger = logger
}

// On registers an event handler for the specified event type.
//
// Parameters:
//   - eventType: The type of event to listen for
//   - handler: The function to call when the event occurs
//
// Example:
//
//	client.On(rabbit.EventConnected, func(event rabbit.Event) {
//		log.Println("Connected to RabbitMQ")
//	})
func (c *Client) On(eventType EventType, handler EventHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventHandlers[eventType] = append(c.eventHandlers[eventType], handler)
}

// emit sends an event to all registered handlers.
//
// Parameters:
//   - eventType: The type of event to emit
//   - data: The event data
func (c *Client) emit(eventType EventType, data interface{}) {
	c.mu.RLock()
	handlers := c.eventHandlers[eventType]
	c.mu.RUnlock()

	event := Event{
		Type: eventType,
		Data: data,
		Time: time.Now(),
	}

	for _, handler := range handlers {
		go handler(event)
	}
}

// initializeMetricsCollection starts the metrics collection goroutine.
func (c *Client) initializeMetricsCollection() {
	c.logger.Debug("Starting metrics collection", "function", "Client.initializeMetricsCollection")

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(DefaultMetricsInterval)
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				c.logger.Debug("Emitting metrics", "function", "Client.initializeMetricsCollection", "metrics", c.metrics)
				c.emit(EventMetrics, *c.metrics)
			}
		}
	}()
}

// startNodeHealthCheck starts periodic health checks for cluster nodes.
func (c *Client) startNodeHealthCheck() {
	interval := c.config.ClusterOptions.NodeRecoveryInterval
	if interval == 0 {
		interval = 30 * time.Second
	}

	c.logger.Debug("Starting node health check",
		"function", "Client.startNodeHealthCheck",
		"interval", interval,
		"hasClusterOptions", c.config.ClusterOptions.NodeRecoveryInterval > 0)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				if err := c.checkClusterNodesHealth(); err != nil {
					c.logger.Error("Node health check interval error",
						"function", "Client.startNodeHealthCheck",
						"error", err.Error())
				}
			}
		}
	}()
}

// Connect establishes a connection to RabbitMQ with automatic failover support.
//
// This method implements connection pooling, circuit breaker pattern, and
// cluster failover strategies. It will attempt to connect to available nodes
// based on the configured failover strategy.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//
// Returns:
//   - error: Error if circuit breaker is open, already reconnecting, or connection fails
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//
//	if err := client.Connect(ctx); err != nil {
//		log.Fatal("Connection failed:", err)
//	}
//	log.Println("Connected to RabbitMQ")
func (c *Client) Connect(ctx context.Context) error {
	c.logger.Info("Initiating RabbitMQ connection",
		"function", "Client.Connect",
		"circuitBreakerOpen", atomic.LoadInt32(&c.circuitBreaker.isOpen) == 1,
		"reconnecting", atomic.LoadInt32(&c.reconnecting) == 1)

	if atomic.LoadInt32(&c.circuitBreaker.isOpen) == 1 {
		err := errors.New("circuit breaker is open")
		c.logger.Error("Connection blocked by circuit breaker",
			"function", "Client.Connect",
			"error", err.Error(),
			"failures", atomic.LoadInt64(&c.circuitBreaker.failures))
		return err
	}

	if atomic.LoadInt32(&c.reconnecting) == 1 {
		err := errors.New("already reconnecting")
		c.logger.Warn("Connection attempt blocked - already reconnecting",
			"function", "Client.Connect",
			"error", err.Error())
		return err
	}

	if c.connection != nil && !c.connection.IsClosed() {
		c.logger.Debug("Connection already established", "function", "Client.Connect")
		return nil
	}

	atomic.StoreInt32(&c.reconnecting, 1)
	defer atomic.StoreInt32(&c.reconnecting, 0)

	c.emit(EventConnecting, nil)
	c.logger.Debug("Starting connection establishment process", "function", "Client.Connect")

	return c.establishConnection(ctx)
}

// establishConnection performs the actual connection establishment with retry logic.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//
// Returns:
//   - error: Error if connection establishment fails
func (c *Client) establishConnection(ctx context.Context) error {
	c.logger.Info("Establishing RabbitMQ connection",
		"function", "Client.establishConnection",
		"reconnectAttempts", atomic.LoadInt32(&c.reconnectAttempts),
		"maxAttempts", c.config.MaxReconnectAttempts)

	var lastErr error
	for attempt := 0; attempt < MaximumInitialConnectionRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		url, err := c.getNextURL()
		if err != nil {
			return err
		}

		c.logger.Debug("Establishing connection attempt",
			"function", "Client.establishConnection",
			"attempt", attempt+1,
			"url", url)

		conn, err := amqp.DialConfig(url, c.buildAMQPConfig())
		if err != nil {
			lastErr = err
			c.logger.Warn("Connection attempt failed",
				"function", "Client.establishConnection",
				"url", url,
				"error", err.Error(),
				"attempt", attempt+1)
			continue
		}

		c.connection = conn
		c.logger.Info("Successfully connected to RabbitMQ",
			"function", "Client.establishConnection",
			"url", url)
		break
	}

	if c.connection == nil {
		err := fmt.Errorf("failed to connect after %d attempts: %w", MaximumInitialConnectionRetries, lastErr)
		c.logger.Error("All connection attempts failed",
			"function", "Client.establishConnection",
			"error", err.Error(),
			"attempts", MaximumInitialConnectionRetries)
		c.handleConnectionError(err)
		return err
	}

	// Setup connection monitoring and handlers
	c.setupConnectionMonitoring()
	c.setupConnectionHandlers()

	// Setup channels
	if err := c.setupChannels(); err != nil {
		c.logger.Error("Failed to setup channels",
			"function", "Client.establishConnection",
			"error", err.Error())
		c.handleConnectionError(err)
		return err
	}

	c.resetCircuitBreakerState()
	c.logger.Info("Successfully established RabbitMQ connection",
		"function", "Client.establishConnection",
		"vhost", c.config.VHost,
		"heartbeat", c.config.Heartbeat)

	c.emit(EventConnected, nil)
	return nil
}

// buildAMQPConfig creates an AMQP configuration from the client config.
//
// Returns:
//   - amqp.Config: The AMQP configuration
func (c *Client) buildAMQPConfig() amqp.Config {
	config := amqp.Config{
		Heartbeat: c.config.Heartbeat,
		Vhost:     c.config.VHost,
	}

	if c.config.SSL.Enabled {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: !c.config.SSL.Validate,
		}
		config.TLSClientConfig = tlsConfig
	}

	return config
}

// getNextURL returns the next URL for connection attempts based on failover strategy.
//
// Returns:
//   - string: Next URL to attempt connection
//   - error: Error if no URLs are configured or URL is invalid
func (c *Client) getNextURL() (string, error) {
	if len(c.config.URLs) == 0 {
		err := errors.New("no RabbitMQ URLs configured")
		c.logger.Error("No URLs configured for connection",
			"function", "Client.getNextURL",
			"error", err.Error())
		return "", err
	}

	currentIndex := atomic.LoadInt32(&c.currentURLIndex)
	var url string

	if c.config.FailoverStrategy == Random {
		url = c.config.URLs[rand.Intn(len(c.config.URLs))]
	} else {
		url = c.config.URLs[currentIndex%int32(len(c.config.URLs))]
		atomic.AddInt32(&c.currentURLIndex, 1)
	}

	c.logger.Debug("Selected URL for connection",
		"function", "Client.getNextURL",
		"url", url,
		"strategy", c.config.FailoverStrategy)

	return url, nil
}

// setupConnectionMonitoring sets up connection monitoring and health checks.
func (c *Client) setupConnectionMonitoring() {
	if c.connection == nil {
		c.logger.Warn("Cannot setup monitoring - no connection",
			"function", "Client.setupConnectionMonitoring")
		return
	}

	c.logger.Debug("Setting up connection monitoring",
		"function", "Client.setupConnectionMonitoring")

	// Monitor connection blocked/unblocked states
	blockedCh := make(chan amqp.Blocking)
	c.connection.NotifyBlocked(blockedCh)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				return
			case blocking := <-blockedCh:
				if blocking.Active {
					c.logger.Warn("Connection blocked by broker",
						"function", "Client.setupConnectionMonitoring",
						"reason", blocking.Reason)
					c.emit(EventBlocked, blocking.Reason)
				} else {
					c.logger.Info("Connection unblocked by broker",
						"function", "Client.setupConnectionMonitoring")
					c.emit(EventUnblocked, nil)
				}
			}
		}
	}()

	// Periodic connection health check
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				if !c.HealthCheck() && atomic.LoadInt32(&c.reconnecting) == 0 {
					c.logger.Warn("Health check failed, initiating reconnection",
						"function", "Client.setupConnectionMonitoring")
					go c.reconnect()
				}
			}
		}
	}()

	c.logger.Debug("Connection monitoring setup completed",
		"function", "Client.setupConnectionMonitoring")
}

// setupConnectionHandlers sets up connection event handlers.
func (c *Client) setupConnectionHandlers() {
	if c.connection == nil {
		err := errors.New("connection not established")
		c.logger.Error("Cannot setup handlers - connection not established",
			"function", "Client.setupConnectionHandlers",
			"error", err.Error())
		return
	}

	c.logger.Debug("Setting up connection event handlers",
		"function", "Client.setupConnectionHandlers")

	// Handle connection close events
	closeCh := make(chan *amqp.Error)
	c.connection.NotifyClose(closeCh)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		select {
		case <-c.ctx.Done():
			return
		case err := <-closeCh:
			if err != nil {
				c.logger.Error("RabbitMQ connection error",
					"function", "Client.setupConnectionHandlers",
					"error", err.Error())
				c.emit(EventConnectionError, err)
			} else {
				c.logger.Warn("RabbitMQ connection closed",
					"function", "Client.setupConnectionHandlers",
					"reconnecting", atomic.LoadInt32(&c.reconnecting) == 1)
				c.emit(EventConnectionClosed, nil)
			}
			go c.reconnect()
		}
	}()

	c.logger.Debug("Connection event handlers setup completed",
		"function", "Client.setupConnectionHandlers")
}

// setupChannels sets up channels including default channel and channel pool.
//
// Returns:
//   - error: Error if connection is not established or channel setup fails
func (c *Client) setupChannels() error {
	if c.connection == nil {
		err := errors.New("connection not established")
		c.logger.Error("Cannot setup channels - no connection",
			"function", "Client.setupChannels",
			"error", err.Error())
		return err
	}

	c.logger.Debug("Setting up channels", "function", "Client.setupChannels")

	// Create default channel
	ch, err := c.connection.Channel()
	if err != nil {
		c.logger.Error("Failed to create default channel",
			"function", "Client.setupChannels",
			"error", err.Error())
		return err
	}

	c.defaultChannel = ch
	if err := c.setupDefaultChannel(); err != nil {
		return err
	}

	// Initialize channel pool
	if err := c.initializeChannelPool(); err != nil {
		return err
	}

	// Start channel recovery monitoring
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				if err := c.checkAndRecoverChannels(); err != nil {
					c.logger.Error("Channel recovery error",
						"function", "Client.setupChannels",
						"error", err.Error())
				}
			}
		}
	}()

	c.logger.Info("Channels setup completed",
		"function", "Client.setupChannels",
		"defaultChannelCreated", c.defaultChannel != nil,
		"poolSize", len(c.channelPool.channels))

	return nil
}

// setupDefaultChannel sets up the default channel with prefetch and event handlers.
//
// Returns:
//   - error: Error if default channel is not established or setup fails
func (c *Client) setupDefaultChannel() error {
	if c.defaultChannel == nil {
		err := errors.New("default channel not established")
		c.logger.Error("Cannot setup default channel - not created",
			"function", "Client.setupDefaultChannel",
			"error", err.Error())
		return err
	}

	c.logger.Debug("Setting up default channel",
		"function", "Client.setupDefaultChannel",
		"prefetchCount", c.config.PrefetchCount,
		"prefetchGlobal", c.config.PrefetchGlobal)

	if c.config.PrefetchCount > 0 {
		if err := c.defaultChannel.Qos(c.config.PrefetchCount, 0, c.config.PrefetchGlobal); err != nil {
			c.logger.Error("Failed to set prefetch on default channel",
				"function", "Client.setupDefaultChannel",
				"error", err.Error())
			return err
		}
		c.logger.Debug("Prefetch configured on default channel",
			"function", "Client.setupDefaultChannel",
			"count", c.config.PrefetchCount,
			"global", c.config.PrefetchGlobal)
	}

	// Setup channel event handlers
	closeCh := make(chan *amqp.Error)
	c.defaultChannel.NotifyClose(closeCh)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		select {
		case <-c.ctx.Done():
			return
		case err := <-closeCh:
			if err != nil {
				c.logger.Error("Default channel error",
					"function", "Client.setupDefaultChannel",
					"error", err.Error())
				c.emit(EventChannelError, err)
			} else {
				c.logger.Debug("Default channel closed",
					"function", "Client.setupDefaultChannel")
				c.emit(EventChannelClosed, nil)
			}
			go c.reconnect()
		}
	}()

	c.logger.Debug("Default channel setup completed",
		"function", "Client.setupDefaultChannel")

	return nil
}

// initializeChannelPool initializes the channel pool with configured number of channels.
//
// Returns:
//   - error: Error if connection is not established or pool initialization fails
func (c *Client) initializeChannelPool() error {
	if c.connection == nil {
		err := errors.New("connection not established")
		c.logger.Error("Cannot initialize channel pool - no connection",
			"function", "Client.initializeChannelPool",
			"error", err.Error())
		return err
	}

	maxChannels := c.config.PoolConfig.MaxChannels
	c.logger.Debug("Initializing channel pool",
		"function", "Client.initializeChannelPool",
		"maxChannels", maxChannels)

	c.channelPool = &ChannelPool{
		channels:   make([]*amqp.Channel, 0, maxChannels),
		inUse:      make(map[*amqp.Channel]bool),
		maxSize:    maxChannels,
		acquireCh:  make(chan struct{}, maxChannels),
		releaseCh:  make(chan *amqp.Channel, maxChannels),
		closeCh:    make(chan struct{}),
		connection: c.connection,
	}

	for i := 0; i < maxChannels; i++ {
		ch, err := c.connection.Channel()
		if err != nil {
			c.logger.Error("Failed to create pool channel",
				"function", "Client.initializeChannelPool",
				"channelIndex", i+1,
				"error", err.Error())
			return err
		}
		c.channelPool.channels = append(c.channelPool.channels, ch)
		c.logger.Debug("Created pool channel",
			"function", "Client.initializeChannelPool",
			"channelIndex", i+1,
			"totalChannels", maxChannels)
	}

	c.logger.Info("Channel pool initialized",
		"function", "Client.initializeChannelPool",
		"channelCount", len(c.channelPool.channels),
		"maxChannels", maxChannels)

	return nil
}

// GetChannel acquires a channel from the pool or creates a new one if available.
//
// This method implements channel pooling with timeout support. If no channels
// are available in the pool, it will wait up to the configured acquire timeout
// for a channel to become available.
//
// Returns:
//   - *amqp.Channel: An available channel from the pool
//   - error: Error if not connected to RabbitMQ or channel acquisition times out
//
// Example:
//
//	channel, err := client.GetChannel()
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.ReleaseChannel(channel)
//
//	// Use channel for operations
//	_, err = channel.QueueDeclare("my-queue", true, false, false, false, nil)
func (c *Client) GetChannel() (*amqp.Channel, error) {
	c.logger.Debug("Acquiring channel from pool",
		"function", "Client.GetChannel",
		"poolSize", len(c.channelPool.channels),
		"inUse", len(c.channelPool.inUse),
		"maxChannels", c.channelPool.maxSize)

	if c.connection == nil || c.connection.IsClosed() {
		err := errors.New("not connected to RabbitMQ")
		c.logger.Error("Channel acquisition failed - no connection",
			"function", "Client.GetChannel",
			"error", err.Error())
		return nil, err
	}

	c.channelPool.mu.Lock()
	defer c.channelPool.mu.Unlock()

	// Try to get an available channel from the pool
	for _, ch := range c.channelPool.channels {
		if !c.channelPool.inUse[ch] && !ch.IsClosed() {
			c.channelPool.inUse[ch] = true
			c.logger.Debug("Acquired existing channel from pool",
				"function", "Client.GetChannel")
			return ch, nil
		}
	}

	// Create new channel if under limit
	if len(c.channelPool.channels) < c.channelPool.maxSize {
		ch, err := c.connection.Channel()
		if err != nil {
			c.logger.Error("Failed to create new channel",
				"function", "Client.GetChannel",
				"error", err.Error())
			return nil, err
		}
		c.channelPool.channels = append(c.channelPool.channels, ch)
		c.channelPool.inUse[ch] = true
		c.logger.Info("Created and acquired new channel",
			"function", "Client.GetChannel",
			"totalChannels", len(c.channelPool.channels))
		return ch, nil
	}

	// Wait for a channel to become available
	c.logger.Debug("Waiting for channel to become available",
		"function", "Client.GetChannel",
		"timeout", c.config.PoolConfig.AcquireTimeout)

	timeout := time.NewTimer(c.config.PoolConfig.AcquireTimeout)
	defer timeout.Stop()

	ticker := time.NewTicker(DefaultChannelCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timeout.C:
			err := errors.New("channel acquisition timeout")
			c.logger.Error("Channel acquisition timed out",
				"function", "Client.GetChannel",
				"error", err.Error(),
				"timeout", c.config.PoolConfig.AcquireTimeout)
			return nil, err
		case <-ticker.C:
			for _, ch := range c.channelPool.channels {
				if !c.channelPool.inUse[ch] && !ch.IsClosed() {
					c.channelPool.inUse[ch] = true
					c.logger.Debug("Acquired channel after waiting",
						"function", "Client.GetChannel")
					return ch, nil
				}
			}
		}
	}
}

// ReleaseChannel releases a channel back to the pool for reuse.
//
// This method marks the channel as available for reuse by other operations.
// The channel should not be used after calling this method.
//
// Parameters:
//   - channel: The channel to release back to the pool
//
// Example:
//
//	channel, err := client.GetChannel()
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.ReleaseChannel(channel)
//
//	// Use channel for operations
func (c *Client) ReleaseChannel(channel *amqp.Channel) {
	c.channelPool.mu.Lock()
	defer c.channelPool.mu.Unlock()

	wasInUse := c.channelPool.inUse[channel]
	delete(c.channelPool.inUse, channel)

	c.logger.Debug("Released channel back to pool",
		"function", "Client.ReleaseChannel",
		"wasInUse", wasInUse,
		"inUseCount", len(c.channelPool.inUse),
		"totalChannels", len(c.channelPool.channels))
}

// Publish publishes a single message with improved async handling.
//
// This method publishes a message to the specified exchange with the given
// routing key. It supports timeout control and automatic error handling.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - options: Publishing options including exchange, routing key, and message properties
//
// Returns:
//   - error: Error if channel is not available or publishing fails
//
// Example:
//
//	err := client.Publish(ctx, &rabbit.PublishOptions{
//		Exchange:   "my-exchange",
//		RoutingKey: "routing.key",
//		Publishing: amqp.Publishing{
//			Body:        []byte("Hello World"),
//			ContentType: "text/plain",
//			Persistent:  true,
//		},
//		Timeout: 5 * time.Second,
//	})
func (c *Client) Publish(ctx context.Context, options *PublishOptions) error {
	c.logger.Debug("Publishing message",
		"function", "Client.Publish",
		"exchange", options.Exchange,
		"routingKey", options.RoutingKey,
		"contentLength", len(options.Publishing.Body),
		"timeout", options.Timeout)

	if err := c.ensureChannel(); err != nil {
		return err
	}

	timeout := options.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	publishCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		err := c.defaultChannel.Publish(
			options.Exchange,
			options.RoutingKey,
			options.Mandatory,
			options.Immediate,
			options.Publishing,
		)
		errCh <- err
	}()

	select {
	case <-publishCtx.Done():
		err := publishCtx.Err()
		c.logger.Error("Message publish timeout",
			"function", "Client.Publish",
			"error", err.Error(),
			"exchange", options.Exchange,
			"routingKey", options.RoutingKey)
		return err
	case err := <-errCh:
		if err != nil {
			c.logger.Error("Message publish failed",
				"function", "Client.Publish",
				"error", err.Error(),
				"exchange", options.Exchange,
				"routingKey", options.RoutingKey)
			c.handleError(err)
			return err
		}
		c.logger.Debug("Message published successfully",
			"function", "Client.Publish",
			"exchange", options.Exchange,
			"routingKey", options.RoutingKey,
			"contentLength", len(options.Publishing.Body))
		c.updateMetrics("sent")
		return nil
	}
}

// PublishBatch publishes a batch of messages to RabbitMQ.
//
// This method publishes multiple messages in a single operation, which can
// be more efficient than publishing messages individually.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - messages: Array of messages to publish
//
// Returns:
//   - error: Error if channel is not available or publishing fails
//
// Example:
//
//	messages := []*rabbit.PublishOptions{
//		{
//			Exchange:   "my-exchange",
//			RoutingKey: "routing.key",
//			Publishing: amqp.Publishing{Body: []byte("message 1")},
//		},
//		{
//			Exchange:   "my-exchange",
//			RoutingKey: "routing.key",
//			Publishing: amqp.Publishing{Body: []byte("message 2")},
//		},
//	}
//	err := client.PublishBatch(ctx, messages)
func (c *Client) PublishBatch(ctx context.Context, messages []*PublishOptions) error {
	c.logger.Debug("Publishing message batch",
		"function", "Client.PublishBatch",
		"messageCount", len(messages))

	if err := c.ensureChannel(); err != nil {
		return err
	}

	for i, msg := range messages {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := c.defaultChannel.Publish(
			msg.Exchange,
			msg.RoutingKey,
			msg.Mandatory,
			msg.Immediate,
			msg.Publishing,
		); err != nil {
			c.logger.Error("Message publish failed in batch",
				"function", "Client.PublishBatch",
				"error", err.Error(),
				"exchange", msg.Exchange,
				"routingKey", msg.RoutingKey,
				"messageIndex", i)
			c.handleError(err)
			return err
		}
		c.logger.Debug("Message published in batch",
			"function", "Client.PublishBatch",
			"exchange", msg.Exchange,
			"routingKey", msg.RoutingKey,
			"messageIndex", i)
	}

	atomic.AddInt64(&c.metrics.MessagesSent, int64(len(messages)))
	c.logger.Info("Batch publish completed",
		"function", "Client.PublishBatch",
		"messageCount", len(messages),
		"totalSent", atomic.LoadInt64(&c.metrics.MessagesSent))

	return nil
}

// Consume consumes messages from a queue with improved async handling.
//
// This method sets up a consumer for the specified queue and calls the provided
// handler function for each received message. The handler function should
// process the message and return an error if processing fails.
//
// Parameters:
//   - ctx: Context for cancellation control
//   - queue: Queue name to consume from
//   - handler: Function to handle received messages
//   - options: Consume options including consumer tag and acknowledgment settings
//
// Returns:
//   - string: Consumer tag for the created consumer
//   - error: Error if channel is not available or consumption setup fails
//
// Example:
//
//	consumerTag, err := client.Consume(ctx, "my-queue", func(msg amqp.Delivery) error {
//		log.Printf("Received: %s", string(msg.Body))
//		return nil // Message will be automatically acknowledged
//	}, &rabbit.ConsumeOptions{
//		AutoAck: false,
//		Timeout: 30 * time.Second,
//	})
func (c *Client) Consume(ctx context.Context, queue string, handler func(amqp.Delivery) error, options *ConsumeOptions) (string, error) {
	c.logger.Debug("Setting up message consumer",
		"function", "Client.Consume",
		"queue", queue,
		"options", options)

	if err := c.ensureChannel(); err != nil {
		return "", err
	}

	if options == nil {
		options = &ConsumeOptions{
			Timeout: 30 * time.Second,
		}
	}

	deliveries, err := c.defaultChannel.Consume(
		queue,
		options.Consumer,
		options.AutoAck,
		options.Exclusive,
		options.NoLocal,
		options.NoWait,
		options.Args,
	)
	if err != nil {
		c.logger.Error("Failed to setup consumer",
			"function", "Client.Consume",
			"error", err.Error(),
			"queue", queue)
		c.handleError(err)
		return "", err
	}

	consumerTag := options.Consumer
	if consumerTag == "" {
		consumerTag = fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case delivery, ok := <-deliveries:
				if !ok {
					c.logger.Debug("Consumer channel closed",
						"function", "Client.Consume",
						"queue", queue,
						"consumerTag", consumerTag)
					return
				}

				c.processMessage(delivery, handler, options)
			}
		}
	}()

	c.logger.Info("Consumer setup completed",
		"function", "Client.Consume",
		"queue", queue,
		"consumerTag", consumerTag)

	return consumerTag, nil
}

// processMessage processes a single message with timeout and error handling.
//
// Parameters:
//   - delivery: The AMQP delivery to process
//   - handler: The message handler function
//   - options: Consume options including timeout settings
func (c *Client) processMessage(delivery amqp.Delivery, handler func(amqp.Delivery) error, options *ConsumeOptions) {
	startTime := time.Now()

	timeout := options.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- handler(delivery)
	}()

	select {
	case <-ctx.Done():
		c.logger.Error("Message processing timeout",
			"function", "Client.processMessage",
			"deliveryTag", delivery.DeliveryTag,
			"timeout", timeout)
		if !options.AutoAck {
			delivery.Nack(false, true) // Requeue the message
		}
		c.handleError(ctx.Err())
	case err := <-errCh:
		processingTime := time.Since(startTime)
		c.updateAvgProcessingTime(processingTime)

		if err != nil {
			c.logger.Error("Message processing failed",
				"function", "Client.processMessage",
				"error", err.Error(),
				"deliveryTag", delivery.DeliveryTag)
			if !options.AutoAck {
				delivery.Nack(false, true) // Requeue the message
			}
			c.handleError(err)
		} else {
			c.logger.Debug("Message processed successfully",
				"function", "Client.processMessage",
				"deliveryTag", delivery.DeliveryTag,
				"processingTime", processingTime)
			if !options.AutoAck {
				delivery.Ack(false)
			}
			c.updateMetrics("received")
		}
	}
}

// AssertQueue asserts a queue exists, creating it if necessary.
//
// This method declares a queue with the specified options. If the queue
// already exists with different options, an error will be returned.
//
// Parameters:
//   - queue: Queue name to assert
//   - options: Queue assertion options
//
// Returns:
//   - amqp.Queue: Information about the asserted queue
//   - error: Error if channel is not available or assertion fails
//
// Example:
//
//	queueInfo, err := client.AssertQueue("my-queue", &rabbit.QueueOptions{
//		Durable:              true,
//		DeadLetterExchange:   "dlx",
//		MessageTTL:           60000,
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	log.Printf("Queue has %d messages", queueInfo.Messages)
func (c *Client) AssertQueue(queue string, options *QueueOptions) (amqp.Queue, error) {
	c.logger.Debug("Asserting queue",
		"function", "Client.AssertQueue",
		"queue", queue,
		"options", options)

	if err := c.ensureChannel(); err != nil {
		return amqp.Queue{}, err
	}

	if options == nil {
		options = &QueueOptions{}
	}

	args := options.Args
	if args == nil {
		args = make(amqp.Table)
	}

	// Add additional queue arguments
	if options.DeadLetterExchange != "" {
		args["x-dead-letter-exchange"] = options.DeadLetterExchange
	}
	if options.DeadLetterRoutingKey != "" {
		args["x-dead-letter-routing-key"] = options.DeadLetterRoutingKey
	}
	if options.MessageTTL > 0 {
		args["x-message-ttl"] = options.MessageTTL
	}
	if options.Expires > 0 {
		args["x-expires"] = options.Expires
	}
	if options.MaxLength > 0 {
		args["x-max-length"] = options.MaxLength
	}
	if options.MaxPriority > 0 {
		args["x-max-priority"] = options.MaxPriority
	}

	result, err := c.defaultChannel.QueueDeclare(
		queue,
		options.Durable,
		options.AutoDelete,
		options.Exclusive,
		options.NoWait,
		args,
	)
	if err != nil {
		c.logger.Error("Failed to assert queue",
			"function", "Client.AssertQueue",
			"error", err.Error(),
			"queue", queue)
		c.handleError(err)
		return amqp.Queue{}, err
	}

	c.logger.Info("Queue asserted successfully",
		"function", "Client.AssertQueue",
		"queue", queue,
		"messageCount", result.Messages,
		"consumerCount", result.Consumers)

	return result, nil
}

// AssertExchange asserts an exchange exists, creating it if necessary.
//
// This method declares an exchange with the specified type and options.
// If the exchange already exists with different options, an error will be returned.
//
// Parameters:
//   - exchange: Exchange name to assert
//   - options: Exchange assertion options including type and properties
//
// Returns:
//   - error: Error if channel is not available or assertion fails
//
// Example:
//
//	err := client.AssertExchange("my-exchange", &rabbit.ExchangeOptions{
//		Kind:              "topic",
//		Durable:           true,
//		AlternateExchange: "alternate-exchange",
//	})
func (c *Client) AssertExchange(exchange string, options *ExchangeOptions) error {
	c.logger.Debug("Asserting exchange",
		"function", "Client.AssertExchange",
		"exchange", exchange,
		"options", options)

	if err := c.ensureChannel(); err != nil {
		return err
	}

	if options == nil {
		options = &ExchangeOptions{Kind: "direct"}
	}

	args := options.Args
	if args == nil {
		args = make(amqp.Table)
	}

	// Add additional exchange arguments
	if options.AlternateExchange != "" {
		args["alternate-exchange"] = options.AlternateExchange
	}

	err := c.defaultChannel.ExchangeDeclare(
		exchange,
		options.Kind,
		options.Durable,
		options.AutoDelete,
		options.Internal,
		options.NoWait,
		args,
	)
	if err != nil {
		c.logger.Error("Failed to assert exchange",
			"function", "Client.AssertExchange",
			"error", err.Error(),
			"exchange", exchange,
			"type", options.Kind)
		c.handleError(err)
		return err
	}

	c.logger.Info("Exchange asserted successfully",
		"function", "Client.AssertExchange",
		"exchange", exchange,
		"type", options.Kind)

	return nil
}

// BindQueue binds a queue to an exchange with a routing pattern.
//
// This method creates a binding between a queue and an exchange using
// the specified routing key pattern.
//
// Parameters:
//   - queue: Queue name to bind
//   - exchange: Exchange name to bind to
//   - routingKey: Routing key pattern for the binding
//
// Returns:
//   - error: Error if channel is not available or binding fails
//
// Example:
//
//	err := client.BindQueue("my-queue", "my-exchange", "routing.key.*")
func (c *Client) BindQueue(queue, exchange, routingKey string) error {
	c.logger.Debug("Binding queue to exchange",
		"function", "Client.BindQueue",
		"queue", queue,
		"exchange", exchange,
		"routingKey", routingKey)

	if err := c.ensureChannel(); err != nil {
		return err
	}

	err := c.defaultChannel.QueueBind(queue, routingKey, exchange, false, nil)
	if err != nil {
		c.logger.Error("Failed to bind queue to exchange",
			"function", "Client.BindQueue",
			"error", err.Error(),
			"queue", queue,
			"exchange", exchange,
			"routingKey", routingKey)
		c.handleError(err)
		return err
	}

	c.logger.Info("Queue bound to exchange successfully",
		"function", "Client.BindQueue",
		"queue", queue,
		"exchange", exchange,
		"routingKey", routingKey)

	return nil
}

// HealthCheck performs a health check on the RabbitMQ connection and channels.
//
// This method verifies that the connection and default channel are healthy
// by performing lightweight operations.
//
// Returns:
//   - bool: True if the client is healthy, false otherwise
//
// Example:
//
//	if !client.HealthCheck() {
//		log.Println("RabbitMQ client is not healthy")
//	}
func (c *Client) HealthCheck() bool {
	c.logger.Debug("Performing health check", "function", "Client.HealthCheck")

	if c.connection == nil || c.connection.IsClosed() {
		c.logger.Debug("Health check failed - no connection",
			"function", "Client.HealthCheck")
		return false
	}

	if c.defaultChannel == nil || c.defaultChannel.IsClosed() {
		c.logger.Debug("Health check failed - no default channel",
			"function", "Client.HealthCheck")
		return false
	}

	// Test channel by doing a lightweight operation
	_, err := c.defaultChannel.QueueDeclarePassive("amq.direct", true, false, false, false, nil)
	if err != nil {
		c.logger.Error("Health check failed",
			"function", "Client.HealthCheck",
			"error", err.Error())
		return false
	}

	c.logger.Debug("Health check passed", "function", "Client.HealthCheck")
	return true
}

// GetMetrics returns current performance and operational metrics.
//
// This method returns a copy of the current metrics including message
// counts, error counts, and performance statistics.
//
// Returns:
//   - Metrics: Copy of current metrics
//
// Example:
//
//	metrics := client.GetMetrics()
//	log.Printf("Messages sent: %d", metrics.MessagesSent)
//	log.Printf("Messages received: %d", metrics.MessagesReceived)
//	log.Printf("Errors: %d", metrics.Errors)
func (c *Client) GetMetrics() Metrics {
	c.logger.Debug("Getting current metrics",
		"function", "Client.GetMetrics",
		"metrics", c.metrics)

	return Metrics{
		MessagesSent:      atomic.LoadInt64(&c.metrics.MessagesSent),
		MessagesReceived:  atomic.LoadInt64(&c.metrics.MessagesReceived),
		Errors:            atomic.LoadInt64(&c.metrics.Errors),
		Reconnections:     atomic.LoadInt64(&c.metrics.Reconnections),
		LastReconnectTime: c.metrics.LastReconnectTime,
		AvgProcessingTime: c.metrics.AvgProcessingTime,
	}
}

// Close performs graceful shutdown of the RabbitMQ client.
//
// This method closes all channels and the connection, and waits for
// in-flight operations to complete.
//
// Returns:
//   - error: Error if shutdown fails
//
// Example:
//
//	if err := client.Close(); err != nil {
//		log.Printf("Error during shutdown: %v", err)
//	}
func (c *Client) Close() error {
	c.logger.Info("Initiating RabbitMQ connection shutdown",
		"function", "Client.Close")

	atomic.StoreInt32(&c.shutdownInProgress, 1)
	atomic.StoreInt32(&c.reconnecting, 0)

	// Cancel context to stop all goroutines
	c.cancel()

	// Close message batch timer
	c.messageBatch.mu.Lock()
	if c.messageBatch.timer != nil {
		c.messageBatch.timer.Stop()
		c.messageBatch.timer = nil
	}
	c.messageBatch.mu.Unlock()

	// Close channel pool
	if c.channelPool != nil {
		c.channelPool.mu.Lock()
		for _, ch := range c.channelPool.channels {
			if ch != nil && !ch.IsClosed() {
				if err := ch.Close(); err != nil {
					c.logger.Error("Error closing pool channel",
						"function", "Client.Close",
						"error", err.Error())
				}
			}
		}
		c.channelPool.channels = nil
		c.channelPool.inUse = nil
		c.channelPool.mu.Unlock()
	}

	// Close default channel
	if c.defaultChannel != nil && !c.defaultChannel.IsClosed() {
		if err := c.defaultChannel.Close(); err != nil {
			c.logger.Error("Error closing default channel",
				"function", "Client.Close",
				"error", err.Error())
		}
		c.defaultChannel = nil
	}

	// Close connection
	if c.connection != nil && !c.connection.IsClosed() {
		if err := c.connection.Close(); err != nil {
			c.logger.Error("Error closing connection",
				"function", "Client.Close",
				"error", err.Error())
		}
		c.connection = nil
	}

	// Wait for all goroutines to finish
	c.wg.Wait()

	c.logger.Info("Successfully closed RabbitMQ connection",
		"function", "Client.Close")
	c.emit(EventClosed, nil)

	return nil
}

// Helper methods

// ensureChannel ensures the default channel is available and connection is healthy.
//
// Returns:
//   - error: Error if not connected or channel is not available
func (c *Client) ensureChannel() error {
	if c.defaultChannel == nil || c.connection == nil ||
		c.connection.IsClosed() || c.defaultChannel.IsClosed() {
		err := errors.New("not connected to RabbitMQ. Call Connect() first")
		c.logger.Error("Channel check failed",
			"function", "Client.ensureChannel",
			"error", err.Error(),
			"hasConnection", c.connection != nil,
			"hasDefaultChannel", c.defaultChannel != nil,
			"connectionClosed", c.connection != nil && c.connection.IsClosed(),
			"channelClosed", c.defaultChannel != nil && c.defaultChannel.IsClosed())
		return err
	}
	return nil
}

// handleError handles errors with logging and metrics updates.
//
// Parameters:
//   - err: The error that occurred
func (c *Client) handleError(err error) {
	atomic.AddInt64(&c.metrics.Errors, 1)

	c.logger.Error("RabbitMQ Error",
		"function", "Client.handleError",
		"error", err.Error(),
		"metrics", c.metrics,
		"connectionState", map[string]interface{}{
			"isConnected":       c.connection != nil && !c.connection.IsClosed(),
			"isReconnecting":    atomic.LoadInt32(&c.reconnecting) == 1,
			"reconnectAttempts": atomic.LoadInt32(&c.reconnectAttempts),
			"circuitBreakerOpen": atomic.LoadInt32(&c.circuitBreaker.isOpen) == 1,
		})

	c.emit(EventError, err)

	if atomic.LoadInt32(&c.reconnecting) == 0 {
		c.logger.Debug("Initiating reconnection due to error",
			"function", "Client.handleError")
		go c.reconnect()
	}
}

// updateMetrics updates metrics for the specified type.
//
// Parameters:
//   - metricType: Type of metric to update ("sent", "received", "error", "reconnect")
func (c *Client) updateMetrics(metricType string) {
	switch metricType {
	case "sent":
		atomic.AddInt64(&c.metrics.MessagesSent, 1)
	case "received":
		atomic.AddInt64(&c.metrics.MessagesReceived, 1)
	case "error":
		atomic.AddInt64(&c.metrics.Errors, 1)
	case "reconnect":
		atomic.AddInt64(&c.metrics.Reconnections, 1)
		now := time.Now()
		c.metrics.LastReconnectTime = &now
	}

	c.logger.Debug("Metrics updated",
		"function", "Client.updateMetrics",
		"type", metricType,
		"currentMetrics", c.metrics)

	// Emit metrics update event asynchronously
	go c.emit(EventMetrics, *c.metrics)
}

// updateAvgProcessingTime updates the average processing time metric.
//
// Parameters:
//   - processingTime: The processing time for the current message
func (c *Client) updateAvgProcessingTime(processingTime time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics.AvgProcessingTime = (c.metrics.AvgProcessingTime + processingTime.Seconds()*1000) / 2
}

// reconnect attempts to reconnect to RabbitMQ with exponential backoff.
func (c *Client) reconnect() {
	if atomic.LoadInt32(&c.shutdownInProgress) == 1 {
		c.logger.Debug("Reconnection skipped - shutdown in progress",
			"function", "Client.reconnect")
		return
	}

	if !atomic.CompareAndSwapInt32(&c.reconnecting, 0, 1) {
		c.logger.Debug("Reconnection already in progress",
			"function", "Client.reconnect")
		return
	}
	defer atomic.StoreInt32(&c.reconnecting, 0)

	c.logger.Info("Starting reconnection process",
		"function", "Client.reconnect",
		"attempt", atomic.LoadInt32(&c.reconnectAttempts)+1,
		"maxAttempts", c.config.MaxReconnectAttempts)

	// Force cleanup of existing connections/channels
	c.forceCleanup()

	for {
		if atomic.LoadInt32(&c.shutdownInProgress) == 1 {
			return
		}

		delay := c.calculateReconnectDelay()
		c.logger.Info("Attempting to reconnect",
			"function", "Client.reconnect",
			"attempt", atomic.LoadInt32(&c.reconnectAttempts)+1,
			"delay", delay,
			"maxAttempts", c.config.MaxReconnectAttempts)

		c.emit(EventReconnecting, nil)

		// Wait for calculated delay
		time.Sleep(delay)

		// Try to establish new connection
		ctx, cancel := context.WithTimeout(context.Background(), c.config.ConnectionTimeout)
		err := c.establishConnection(ctx)
		cancel()

		if err == nil {
			atomic.StoreInt32(&c.reconnectAttempts, 0)
			c.updateMetrics("reconnect")
			c.logger.Info("Successfully reconnected",
				"function", "Client.reconnect",
				"totalReconnections", atomic.LoadInt64(&c.metrics.Reconnections))
			c.emit(EventReconnected, nil)
			return
		}

		atomic.AddInt32(&c.reconnectAttempts, 1)
		c.logger.Error("Reconnection attempt failed",
			"function", "Client.reconnect",
			"error", err.Error(),
			"attempt", atomic.LoadInt32(&c.reconnectAttempts),
			"maxAttempts", c.config.MaxReconnectAttempts)

		if c.config.MaxReconnectAttempts != -1 &&
			int(atomic.LoadInt32(&c.reconnectAttempts)) >= c.config.MaxReconnectAttempts {
			c.logger.Error("Max reconnection attempts exceeded",
				"function", "Client.reconnect",
				"attempts", atomic.LoadInt32(&c.reconnectAttempts),
				"maxAttempts", c.config.MaxReconnectAttempts)
			c.emit(EventReconnectFailed, err)
			return
		}
	}
}

// calculateReconnectDelay calculates the delay for the next reconnection attempt.
//
// Returns:
//   - time.Duration: Delay for the next reconnection attempt
func (c *Client) calculateReconnectDelay() time.Duration {
	baseDelay := c.config.ReconnectDelay
	maxDelay := 60 * time.Second

	if !c.config.ExponentialBackoff {
		c.logger.Debug("Using fixed reconnect delay",
			"function", "Client.calculateReconnectDelay",
			"delay", baseDelay)
		return baseDelay
	}

	// Calculate exponential backoff with jitter
	attempts := atomic.LoadInt32(&c.reconnectAttempts)
	exponentialDelay := time.Duration(float64(baseDelay) * math.Pow(2, float64(attempts)))
	if exponentialDelay > maxDelay {
		exponentialDelay = maxDelay
	}

	// Add random jitter (±20%)
	jitter := time.Duration(float64(exponentialDelay) * 0.2 * (rand.Float64()*2 - 1))
	finalDelay := exponentialDelay + jitter
	if finalDelay < baseDelay {
		finalDelay = baseDelay
	}
	if finalDelay > maxDelay {
		finalDelay = maxDelay
	}

	c.logger.Debug("Calculated exponential backoff delay",
		"function", "Client.calculateReconnectDelay",
		"baseDelay", baseDelay,
		"exponentialDelay", exponentialDelay,
		"jitter", jitter,
		"finalDelay", finalDelay,
		"attempt", attempts)

	return finalDelay
}

// forceCleanup forces cleanup of all connections and channels.
func (c *Client) forceCleanup() {
	c.logger.Debug("Starting force cleanup of connections and channels",
		"function", "Client.forceCleanup")

	// Force close all channels in the pool
	if c.channelPool != nil {
		c.channelPool.mu.Lock()
		for _, ch := range c.channelPool.channels {
			if ch != nil && !ch.IsClosed() {
				ch.Close() // Ignore errors during cleanup
			}
		}
		c.channelPool.channels = nil
		c.channelPool.inUse = make(map[*amqp.Channel]bool)
		c.channelPool.mu.Unlock()
	}

	// Force close default channel
	if c.defaultChannel != nil && !c.defaultChannel.IsClosed() {
		c.defaultChannel.Close() // Ignore errors during cleanup
		c.defaultChannel = nil
	}

	// Force close connection
	if c.connection != nil && !c.connection.IsClosed() {
		c.connection.Close() // Ignore errors during cleanup
		c.connection = nil
	}

	c.logger.Debug("Force cleanup completed", "function", "Client.forceCleanup")
}

// checkAndRecoverChannels checks and recovers failed channels.
//
// Returns:
//   - error: Error if channel recovery fails
func (c *Client) checkAndRecoverChannels() error {
	if c.connection == nil || c.connection.IsClosed() {
		c.logger.Debug("Cannot recover channels - connection is not open",
			"function", "Client.checkAndRecoverChannels")
		return nil
	}

	c.logger.Debug("Checking channel health",
		"function", "Client.checkAndRecoverChannels")

	// Check and recover default channel
	if c.defaultChannel == nil || c.defaultChannel.IsClosed() {
		c.logger.Info("Attempting to recover default channel",
			"function", "Client.checkAndRecoverChannels")

		maxRetries := c.config.ChannelOptions.MaxRetries
		if maxRetries == 0 {
			maxRetries = 3
		}
		retryDelay := c.config.ChannelOptions.RetryDelay
		if retryDelay == 0 {
			retryDelay = 1 * time.Second
		}

		for attempt := 0; attempt < maxRetries; attempt++ {
			ch, err := c.connection.Channel()
			if err != nil {
				c.logger.Error("Failed to recover default channel",
					"function", "Client.checkAndRecoverChannels",
					"attempt", attempt+1,
					"maxRetries", maxRetries,
					"error", err.Error())
				if attempt < maxRetries-1 {
					time.Sleep(retryDelay)
					continue
				}
				return err
			}

			c.defaultChannel = ch
			if err := c.setupDefaultChannel(); err != nil {
				c.logger.Error("Failed to setup recovered default channel",
					"function", "Client.checkAndRecoverChannels",
					"error", err.Error())
				if attempt < maxRetries-1 {
					time.Sleep(retryDelay)
					continue
				}
				return err
			}

			c.logger.Info("Successfully recovered default channel",
				"function", "Client.checkAndRecoverChannels")
			break
		}
	}

	// Check and recover pool channels
	if c.channelPool != nil {
		c.channelPool.mu.Lock()
		for i, ch := range c.channelPool.channels {
			if ch == nil || ch.IsClosed() {
				c.logger.Info("Attempting to recover pool channel",
					"function", "Client.checkAndRecoverChannels",
					"channelIndex", i)

				newCh, err := c.connection.Channel()
				if err != nil {
					c.logger.Error("Failed to recover pool channel",
						"function", "Client.checkAndRecoverChannels",
						"channelIndex", i,
						"error", err.Error())
					continue
				}

				// Update pool
				wasInUse := c.channelPool.inUse[ch]
				delete(c.channelPool.inUse, ch)
				c.channelPool.channels[i] = newCh
				if wasInUse {
					c.channelPool.inUse[newCh] = true
				}

				c.logger.Info("Successfully recovered pool channel",
					"function", "Client.checkAndRecoverChannels",
					"channelIndex", i)
			}
		}
		c.channelPool.mu.Unlock()
	}

	return nil
}

// resetCircuitBreakerState resets circuit breaker state after successful connection.
func (c *Client) resetCircuitBreakerState() {
	c.circuitBreaker.mu.Lock()
	defer c.circuitBreaker.mu.Unlock()

	wasOpen := atomic.LoadInt32(&c.circuitBreaker.isOpen) == 1
	atomic.StoreInt64(&c.circuitBreaker.failures, 0)
	atomic.StoreInt32(&c.circuitBreaker.isOpen, 0)
	c.circuitBreaker.lastFailure = time.Time{}

	if wasOpen {
		c.logger.Info("Circuit breaker reset after successful connection",
			"function", "Client.resetCircuitBreakerState")
	}
}

// checkClusterNodesHealth checks the health of all cluster nodes.
//
// Returns:
//   - error: Error if health check fails
func (c *Client) checkClusterNodesHealth() error {
	if len(c.config.URLs) == 0 {
		c.logger.Debug("No URLs configured for cluster health check",
			"function", "Client.checkClusterNodesHealth")
		return nil
	}

	c.logger.Debug("Starting cluster nodes health check",
		"function", "Client.checkClusterNodesHealth",
		"nodeCount", len(c.config.URLs))

	for _, url := range c.config.URLs {
		go func(nodeURL string) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, err := amqp.DialConfig(nodeURL, c.buildAMQPConfig())
			if err != nil {
				c.mu.Lock()
				status := c.activeNodes[nodeURL]
				if status == nil {
					status = &NodeStatus{Healthy: true, LastChecked: time.Now(), FailureCount: 0}
				}
				status.FailureCount++
				status.Healthy = status.FailureCount < 3
				status.LastChecked = time.Now()
				c.activeNodes[nodeURL] = status
				c.mu.Unlock()

				c.logger.Warn("Cluster node health check failed",
					"function", "Client.checkClusterNodesHealth",
					"url", nodeURL,
					"error", err.Error(),
					"status", status)
				return
			}

			conn.Close()

			c.mu.Lock()
			c.activeNodes[nodeURL] = &NodeStatus{
				Healthy:      true,
				LastChecked:  time.Now(),
				FailureCount: 0,
			}
			c.mu.Unlock()

			c.logger.Debug("Cluster node health check passed",
				"function", "Client.checkClusterNodesHealth",
				"url", nodeURL)
		}(url)
	}

	return nil
}

// handleConnectionError handles connection errors and updates circuit breaker state.
//
// Parameters:
//   - err: The error that occurred
func (c *Client) handleConnectionError(err error) {
	c.circuitBreaker.mu.Lock()
	atomic.AddInt64(&c.circuitBreaker.failures, 1)
	failures := atomic.LoadInt64(&c.circuitBreaker.failures)
	
	if failures >= int64(c.circuitBreaker.config.FailureThreshold) {
		atomic.StoreInt32(&c.circuitBreaker.isOpen, 1)
	}
	c.circuitBreaker.lastFailure = time.Now()
	c.circuitBreaker.mu.Unlock()

	c.logger.Error("Connection error handled",
		"function", "Client.handleConnectionError",
		"error", err.Error(),
		"failures", failures,
		"circuitBreakerOpen", atomic.LoadInt32(&c.circuitBreaker.isOpen) == 1,
		"threshold", c.circuitBreaker.config.FailureThreshold)

	c.emit(EventConnectionFailed, err)
} 