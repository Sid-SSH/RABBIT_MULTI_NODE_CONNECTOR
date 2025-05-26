# RabbitMQ Client for Go

A comprehensive, enterprise-grade RabbitMQ client library for Go applications with advanced features including connection pooling, circuit breaker pattern, cluster support, and automatic failover.

## Features

- **Connection Pooling**: Efficient channel management with configurable pool sizes
- **Circuit Breaker**: Fault tolerance with automatic circuit breaking on failures
- **Cluster Support**: Multi-node cluster support with failover strategies
- **Exponential Backoff**: Smart reconnection with exponential backoff and jitter
- **Health Monitoring**: Built-in health checks and metrics collection
- **Event System**: Comprehensive event system for monitoring client state
- **Graceful Shutdown**: Proper resource cleanup and in-flight message handling
- **Structured Logging**: Integration with Go's structured logging (slog)
- **Context Support**: Full context support for cancellation and timeouts
- **Type Safety**: Comprehensive type definitions with JSON serialization support

## Installation

```bash
go get github.com/sid/rabbitTEST/src/rabbit
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/streadway/amqp"
    "github.com/sid/rabbitTEST/src/rabbit"
)

func main() {
    // Create client configuration
    config := &rabbit.Config{
        URLs:     []string{"amqp://localhost:5672"},
        Heartbeat: 60 * time.Second,
        PoolConfig: rabbit.PoolConfig{
            MaxChannels:    10,
            AcquireTimeout: 5 * time.Second,
        },
        CircuitBreaker: rabbit.CircuitBreakerConfig{
            FailureThreshold: 5,
            ResetTimeout:     30 * time.Second,
        },
    }

    // Create client
    client, err := rabbit.NewClient(config)
    if err != nil {
        log.Fatal("Failed to create client:", err)
    }
    defer client.Close()

    // Connect to RabbitMQ
    ctx := context.Background()
    if err := client.Connect(ctx); err != nil {
        log.Fatal("Failed to connect:", err)
    }

    // Publish a message
    err = client.Publish(ctx, &rabbit.PublishOptions{
        Exchange:   "my-exchange",
        RoutingKey: "routing.key",
        Publishing: amqp.Publishing{
            Body:        []byte("Hello World"),
            ContentType: "text/plain",
            Persistent:  true,
        },
    })
    if err != nil {
        log.Fatal("Failed to publish:", err)
    }

    log.Println("Message published successfully")
}
```

## Advanced Usage

### Cluster Configuration

```go
config := &rabbit.Config{
    URLs: []string{
        "amqp://node1.rabbitmq.local:5672",
        "amqp://node2.rabbitmq.local:5672",
        "amqp://node3.rabbitmq.local:5672",
    },
    FailoverStrategy: rabbit.RoundRobin, // or rabbit.Random
    ClusterOptions: rabbit.ClusterOptions{
        RetryConnectTimeout:  5 * time.Second,
        NodeRecoveryInterval: 30 * time.Second,
        ShuffleNodes:         true,
        PriorityNodes:        []string{"amqp://node1.rabbitmq.local:5672"},
    },
}
```

### Message Consumption

```go
// Setup consumer
consumerTag, err := client.Consume(ctx, "my-queue", func(delivery amqp.Delivery) error {
    log.Printf("Received message: %s", string(delivery.Body))
    
    // Process message here
    // Return error if processing fails (message will be nacked and requeued)
    return nil
}, &rabbit.ConsumeOptions{
    AutoAck:   false,
    Exclusive: false,
    Timeout:   30 * time.Second,
})
if err != nil {
    log.Fatal("Failed to setup consumer:", err)
}

log.Printf("Consumer started with tag: %s", consumerTag)
```

### Batch Publishing

```go
messages := []*rabbit.PublishOptions{
    {
        Exchange:   "my-exchange",
        RoutingKey: "routing.key.1",
        Publishing: amqp.Publishing{Body: []byte("Message 1")},
    },
    {
        Exchange:   "my-exchange",
        RoutingKey: "routing.key.2",
        Publishing: amqp.Publishing{Body: []byte("Message 2")},
    },
}

err := client.PublishBatch(ctx, messages)
if err != nil {
    log.Fatal("Failed to publish batch:", err)
}
```

### Queue and Exchange Management

```go
// Assert queue with advanced options
queueInfo, err := client.AssertQueue("my-queue", &rabbit.QueueOptions{
    Durable:              true,
    DeadLetterExchange:   "dlx",
    DeadLetterRoutingKey: "failed",
    MessageTTL:           60000, // 60 seconds
    MaxLength:            1000,
    MaxPriority:          10,
})
if err != nil {
    log.Fatal("Failed to assert queue:", err)
}

// Assert exchange
err = client.AssertExchange("my-exchange", &rabbit.ExchangeOptions{
    Kind:              "topic",
    Durable:           true,
    AlternateExchange: "alternate-exchange",
})
if err != nil {
    log.Fatal("Failed to assert exchange:", err)
}

// Bind queue to exchange
err = client.BindQueue("my-queue", "my-exchange", "routing.key.*")
if err != nil {
    log.Fatal("Failed to bind queue:", err)
}
```

### Event Handling

```go
// Register event handlers
client.On(rabbit.EventConnected, func(event rabbit.Event) {
    log.Println("Connected to RabbitMQ")
})

client.On(rabbit.EventConnectionError, func(event rabbit.Event) {
    log.Printf("Connection error: %v", event.Data)
})

client.On(rabbit.EventMetrics, func(event rabbit.Event) {
    metrics := event.Data.(rabbit.Metrics)
    log.Printf("Metrics - Sent: %d, Received: %d, Errors: %d",
        metrics.MessagesSent, metrics.MessagesReceived, metrics.Errors)
})

client.On(rabbit.EventReconnecting, func(event rabbit.Event) {
    log.Println("Attempting to reconnect...")
})

client.On(rabbit.EventReconnected, func(event rabbit.Event) {
    log.Println("Successfully reconnected")
})
```

### Health Monitoring

```go
// Check client health
if !client.HealthCheck() {
    log.Println("Client is not healthy")
}

// Get current metrics
metrics := client.GetMetrics()
log.Printf("Messages sent: %d", metrics.MessagesSent)
log.Printf("Messages received: %d", metrics.MessagesReceived)
log.Printf("Errors: %d", metrics.Errors)
log.Printf("Reconnections: %d", metrics.Reconnections)
log.Printf("Avg processing time: %.2f ms", metrics.AvgProcessingTime)
```

### Channel Pool Management

```go
// Get channel from pool
channel, err := client.GetChannel()
if err != nil {
    log.Fatal("Failed to get channel:", err)
}
defer client.ReleaseChannel(channel)

// Use channel for operations
_, err = channel.QueueDeclare("temp-queue", false, true, false, false, nil)
if err != nil {
    log.Fatal("Failed to declare queue:", err)
}
```

### SSL/TLS Configuration

```go
config := &rabbit.Config{
    URLs: []string{"amqps://localhost:5671"},
    SSL: rabbit.SSLConfig{
        Enabled:  true,
        Validate: true,
        CA:       []string{"/path/to/ca.pem"},
        Cert:     "/path/to/client.pem",
        Key:      "/path/to/client.key",
    },
}
```

### Custom Logging

```go
import "log/slog"

// Create custom logger
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))

// Set custom logger
client.SetLogger(logger)
```

## Configuration Options

### Config

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `URLs` | `[]string` | Array of RabbitMQ node URLs | Required |
| `Heartbeat` | `time.Duration` | Heartbeat interval | 60s |
| `ConnectionName` | `string` | Human-readable connection name | "" |
| `PrefetchCount` | `int` | Message prefetch count | 0 |
| `PrefetchGlobal` | `bool` | Whether prefetch applies globally | false |
| `ReconnectDelay` | `time.Duration` | Delay between reconnection attempts | 5s |
| `MaxReconnectAttempts` | `int` | Max reconnection attempts (-1 = unlimited) | -1 |
| `ExponentialBackoff` | `bool` | Use exponential backoff for reconnections | true |
| `ConnectionTimeout` | `time.Duration` | Connection timeout | 30s |
| `FailoverStrategy` | `FailoverStrategy` | Node selection strategy | RoundRobin |
| `VHost` | `string` | Virtual host | "/" |
| `Username` | `string` | Authentication username | "guest" |
| `Password` | `string` | Authentication password | "guest" |

### PoolConfig

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `MaxChannels` | `int` | Maximum channels in pool | 10 |
| `AcquireTimeout` | `time.Duration` | Timeout for acquiring channel | 5s |

### CircuitBreakerConfig

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `FailureThreshold` | `int` | Failures before opening circuit | 5 |
| `ResetTimeout` | `time.Duration` | Time before attempting to close circuit | 30s |

### ClusterOptions

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `RetryConnectTimeout` | `time.Duration` | Time to wait before trying next node | 5s |
| `NodeRecoveryInterval` | `time.Duration` | Interval for node health checks | 30s |
| `ShuffleNodes` | `bool` | Randomly shuffle nodes on startup | false |
| `PriorityNodes` | `[]string` | Preferred nodes to try first | nil |

## Events

The client emits various events that you can listen to:

- `EventConnecting`: Starting connection attempt
- `EventConnected`: Successfully connected
- `EventConnectionError`: Connection error occurred
- `EventConnectionClosed`: Connection closed
- `EventConnectionFailed`: Connection attempt failed
- `EventChannelError`: Channel error occurred
- `EventChannelClosed`: Channel closed
- `EventReconnecting`: Starting reconnection attempt
- `EventReconnected`: Successfully reconnected
- `EventReconnectFailed`: Reconnection failed
- `EventError`: General error
- `EventClosed`: Client closed
- `EventBlocked`: Connection blocked by broker
- `EventUnblocked`: Connection unblocked by broker
- `EventMetrics`: Periodic metrics update

## Error Handling

The client implements comprehensive error handling with automatic recovery:

1. **Connection Errors**: Automatic reconnection with exponential backoff
2. **Channel Errors**: Automatic channel recovery and pool management
3. **Circuit Breaker**: Prevents cascading failures by opening circuit after threshold
4. **Cluster Failover**: Automatic failover to healthy nodes in cluster
5. **Timeout Handling**: Configurable timeouts for all operations

## Best Practices

1. **Always use context**: Pass context for cancellation and timeout control
2. **Handle events**: Register event handlers for monitoring and alerting
3. **Configure timeouts**: Set appropriate timeouts for your use case
4. **Use connection pooling**: Leverage the built-in channel pool for efficiency
5. **Monitor metrics**: Regularly check metrics for performance insights
6. **Graceful shutdown**: Always call `Close()` to ensure proper cleanup
7. **Error handling**: Implement proper error handling and retry logic
8. **Health checks**: Use `HealthCheck()` for monitoring client health

## Thread Safety

The client is fully thread-safe and can be used concurrently from multiple goroutines. All operations are protected by appropriate synchronization mechanisms.

## Performance Considerations

- Use batch publishing for high-throughput scenarios
- Configure appropriate pool sizes based on your workload
- Monitor metrics to identify bottlenecks
- Use appropriate prefetch counts for consumers
- Consider cluster configuration for high availability

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For support and questions, please open an issue on the GitHub repository. 