/**
 * @fileoverview RabbitMQ Client Provider with connection pooling and circuit breaker
 * @version 1.0.0
 * @author SID
 * @since 1.0.0
 */

import { Buffer } from 'buffer';
import { EventEmitter } from 'events';
import {
  setTimeout,
  setInterval,
  clearTimeout,
  clearInterval,
} from 'node:timers';

import * as amqplib from 'amqplib';

import logger from './logger.js';

import type {
  ConfirmChannel,
  Channel,
  Connection,
  Options,
  Message,
} from 'amqplib';

// Type assertion for amqplib connection with createConfirmChannel method
interface AmqpConnection extends Connection {
  createConfirmChannel(): Promise<ConfirmChannel>;
  createChannel(): Promise<Channel>;
  close(): Promise<void>;
}

/**
 * Event types for RabbitMQ client with better type safety
 * @interface RabbitMQEvents
 */
interface RabbitMQEvents {
  /** Emitted when starting connection attempt */
  connecting: () => void;
  /** Emitted when successfully connected */
  connected: () => void;
  /** Emitted when connection error occurs */
  connectionError: (error: Error) => void;
  /** Emitted when connection is closed */
  connectionClosed: () => void;
  /** Emitted when connection attempt fails */
  connectionFailed: (error: Error) => void;
  /** Emitted when channel error occurs */
  channelError: (error: Error) => void;
  /** Emitted when channel is closed */
  channelClosed: () => void;
  /** Emitted when channel drain event occurs */
  channelDrain: () => void;
  /** Emitted when message is returned by broker */
  messageReturned: (msg: Message) => void;
  /** Emitted periodically with current metrics */
  metrics: (metrics: Metrics) => void;
  /** Emitted when starting reconnection attempt */
  reconnecting: () => void;
  /** Emitted when successfully reconnected */
  reconnected: () => void;
  /** Emitted when reconnection fails */
  reconnectFailed: (error: Error) => void;
  /** Emitted on general errors */
  error: (error: Error) => void;
  /** Emitted when client is closed */
  closed: () => void;
  /** Emitted when connection is blocked by broker */
  blocked: (reason: string) => void;
  /** Emitted when connection is unblocked by broker */
  unblocked: () => void;
}

/**
 * Event emitter interface for RabbitMQ client
 * @interface RabbitMQClientEvents
 */
interface RabbitMQClientEvents {
  on<E extends keyof RabbitMQEvents>(
    event: E,
    listener: RabbitMQEvents[E],
  ): this;
  emit<E extends keyof RabbitMQEvents>(
    event: E,
    ...args: Parameters<RabbitMQEvents[E]>
  ): boolean;
}

/**
 * Validation constants for configuration parameters
 * @constant {Object} CONSTANTS
 */
const CONSTANTS = {
  /** Minimum heartbeat interval in seconds */
  MIN_HEARTBEAT: 1,
  /** Maximum heartbeat interval in seconds */
  MAX_HEARTBEAT: 60,
  /** Minimum reconnect delay in milliseconds */
  MIN_RECONNECT_DELAY: 1000,
  /** Maximum reconnect delay in milliseconds */
  MAX_RECONNECT_DELAY: 60000,
  /** Default channel availability check interval */
  DEFAULT_CHANNEL_CHECK_INTERVAL: 100,
  /** Default metrics emission interval */
  DEFAULT_METRICS_INTERVAL: 60000,
  /** Maximum initial connection retry attempts */
  MAXIMUM_INITIAL_CONNECTION_RETRIES: 5,
} as const;

/**
 * Socket configuration options
 * @interface SocketOptions
 */
interface SocketOptions {
  /** Socket timeout in milliseconds */
  timeout?: number;
  /** Disable Nagle's algorithm */
  noDelay?: boolean;
}

/**
 * Client properties for connection identification
 * @interface ClientProperties
 */
interface ClientProperties {
  /** Application name for identification */
  applicationName?: string;
  /** Client capabilities */
  capabilities?: Record<string, unknown>;
}

/**
 * Performance and operational metrics
 * @interface Metrics
 */
interface Metrics {
  /** Total number of messages sent */
  messagesSent: number;
  /** Total number of messages received */
  messagesReceived: number;
  /** Total number of errors encountered */
  errors: number;
  /** Total number of reconnection attempts */
  reconnections: number;
  /** Timestamp of last reconnection */
  lastReconnectTime: Date | null;
  /** Average message processing time in milliseconds */
  avgProcessingTime: number;
}

/**
 * Circuit breaker configuration for fault tolerance
 * @interface CircuitBreakerConfig
 */
interface CircuitBreakerConfig {
  /** Number of failures before opening circuit */
  failureThreshold: number;
  /** Time in milliseconds before attempting to close circuit */
  resetTimeout: number;
}

/**
 * Channel pool configuration and state
 * @interface ChannelPool
 */
interface ChannelPool {
  /** Array of available channels */
  channels: (Channel | ConfirmChannel)[];
  /** Maximum number of channels in pool */
  maxChannels: number;
  /** Set of channels currently in use */
  inUse: Set<Channel | ConfirmChannel>;
}

/**
 * Comprehensive RabbitMQ client configuration options
 * @interface RabbitMQOptions
 */
interface RabbitMQOptions {
  /** Connection URL or connection options */
  url?: string | Options.Connect;
  /** Socket-level configuration */
  socketOptions?: SocketOptions;
  /** Client identification properties */
  clientProperties?: ClientProperties;
  /** Heartbeat interval in seconds */
  heartbeat?: number;
  /** Human-readable connection name for debugging */
  connectionName?: string;
  /** Per-channel message prefetch count */
  prefetchCount?: number;
  /** Whether prefetch applies globally */
  prefetchGlobal?: boolean;
  /** Delay between reconnection attempts in milliseconds */
  reconnectDelay?: number;
  /** Maximum number of reconnection attempts (-1 for unlimited) */
  maxReconnectAttempts?: number;
  /** Whether to use exponential backoff for reconnection delays */
  exponentialBackoff?: boolean;
  /** Channel pool configuration */
  poolConfig?: {
    /** Maximum channels in pool */
    maxChannels: number;
    /** Timeout for acquiring channel from pool */
    acquireTimeout: number;
  };
  /** Circuit breaker configuration */
  circuitBreaker?: CircuitBreakerConfig;
  /** Message batching configuration */
  batchConfig?: {
    /** Maximum messages per batch */
    size: number;
    /** Maximum time to wait before sending batch */
    timeoutMs: number;
  };
  /** Array of cluster node URLs */
  urls?: string[];
  /** Connection timeout in milliseconds */
  connectionTimeout?: number;
  /** Strategy for selecting cluster nodes */
  failoverStrategy?: 'round-robin' | 'random';
  /** Virtual host name */
  vhost?: string;
  /** SSL/TLS configuration */
  ssl?: {
    /** Whether SSL is enabled */
    enabled: boolean;
    /** Whether to validate server certificate */
    validate?: boolean;
    /** Certificate authority certificates */
    ca?: string[];
    /** Client certificate */
    cert?: string;
    /** Client private key */
    key?: string;
    /** Private key passphrase */
    passphrase?: string;
  };
  /** Cluster-specific options */
  clusterOptions?: {
    /** Time to wait before trying next node */
    retryConnectTimeout?: number;
    /** Interval for node health checks */
    nodeRecoveryInterval?: number;
    /** Whether to randomly shuffle nodes on startup */
    shuffleNodes?: boolean;
    /** Preferred nodes to try first */
    priorityNodes?: string[];
  };
  /** Channel recovery options */
  channelOptions?: {
    /** Maximum channel recovery retries */
    maxRetries: number;
    /** Delay between channel recovery attempts */
    retryDelay: number;
    /** Whether to automatically recover channels */
    autoRecovery: boolean;
  };
}

/**
 * Queue assertion options with additional RabbitMQ features
 * @interface QueueOptions
 * @extends Options.AssertQueue
 */
export interface QueueOptions extends Options.AssertQueue {
  /** Dead letter exchange name */
  deadLetterExchange?: string;
  /** Dead letter routing key */
  deadLetterRoutingKey?: string;
  /** Message time-to-live in milliseconds */
  messageTtl?: number;
  /** Queue expiration time in milliseconds */
  expires?: number;
  /** Maximum queue length */
  maxLength?: number;
  /** Maximum message priority */
  maxPriority?: number;
}

/**
 * Exchange assertion options with additional RabbitMQ features
 * @interface ExchangeOptions
 * @extends Options.AssertExchange
 */
export interface ExchangeOptions extends Options.AssertExchange {
  /** Alternate exchange for unroutable messages */
  alternateExchange?: string;
}

/**
 * Message batch for bulk publishing
 * @interface MessageBatch
 */
interface MessageBatch {
  /** Array of messages to be published */
  messages: {
    /** Target exchange name */
    exchange: string;
    /** Message routing key */
    routingKey: string;
    /** Message content as Buffer */
    content: Buffer;
    /** Publishing options */
    options: Options.Publish;
  }[];
  /** Timer for batch timeout */
  timer: ReturnType<typeof setTimeout> | null;
}

/**
 * Extended connection options with timeout support
 * @interface ExtendedConnectOptions
 * @extends Options.Connect
 */
interface ExtendedConnectOptions extends Options.Connect {
  /** Connection timeout in milliseconds */
  timeout?: number;
}

/**
 * Node status tracking for cluster management
 * @interface NodeStatus
 */
interface NodeStatus {
  /** Whether the node is currently healthy */
  healthy: boolean;
  /** Last health check timestamp */
  lastChecked: Date;
  /** Number of consecutive failures */
  failureCount: number;
}

/**
 * Advanced RabbitMQ client with connection pooling, circuit breaker, and cluster support
 * 
 * @class RabbitMQClient
 * @extends EventEmitter
 * @implements RabbitMQClientEvents
 * 
 * @example
 * ```typescript
 * const client = new RabbitMQClient({
 *   urls: ['amqp://localhost:5672'],
 *   heartbeat: 60,
 *   reconnectDelay: 5000,
 *   poolConfig: { maxChannels: 10, acquireTimeout: 5000 }
 * });
 * 
 * await client.connect();
 * await client.publish('exchange', 'routing.key', Buffer.from('message'));
 * await client.close();
 * ```
 * 
 * @fires RabbitMQClient#connecting
 * @fires RabbitMQClient#connected
 * @fires RabbitMQClient#connectionError
 * @fires RabbitMQClient#connectionClosed
 * @fires RabbitMQClient#error
 * @fires RabbitMQClient#metrics
 */
class RabbitMQClient extends EventEmitter implements RabbitMQClientEvents {
  /** Current RabbitMQ connection instance */
  private connection: AmqpConnection | null = null;
  
  /** Default confirm channel for operations */
  private defaultChannel: ConfirmChannel | null = null;
  
  /** Client configuration options */
  private readonly options: RabbitMQOptions;
  
  /** Whether reconnection is in progress */
  private reconnecting = false;
  
  /** Promise for current connection attempt */
  private connectionPromise: Promise<void> | null = null;
  
  /** Number of reconnection attempts made */
  private reconnectAttempts = 0;

  /** Channel pool for managing multiple channels */
  private readonly channelPool: ChannelPool = {
    channels: [],
    maxChannels: 10,
    inUse: new Set(),
  };

  /** Performance and operational metrics */
  private readonly metrics: Metrics = {
    messagesSent: 0,
    messagesReceived: 0,
    errors: 0,
    reconnections: 0,
    lastReconnectTime: null,
    avgProcessingTime: 0,
  };

  /** Circuit breaker state for fault tolerance */
  private readonly circuitBreaker = {
    failures: 0,
    isOpen: false,
    lastFailure: null as Date | null,
  };

  /** Current message batch for bulk operations */
  private messageBatch: MessageBatch = {
    messages: [],
    timer: null,
  };

  /** Current URL index for round-robin failover */
  private currentUrlIndex = 0;

  /** Whether shutdown is in progress */
  private shutdownInProgress = false;

  /** Cluster node status tracking */
  private activeNodes: Map<string, NodeStatus> = new Map();

  /**
   * Initializes a new RabbitMQ client with the specified options
   * 
   * @param {RabbitMQOptions} options - Configuration options for the RabbitMQ client
   * @throws {Error} When options validation fails
   * 
   * @example
   * ```typescript
   * const client = new RabbitMQClient({
   *   urls: ['amqp://localhost:5672'],
   *   heartbeat: 60,
   *   reconnectDelay: 5000
   * });
   * ```
   */
  constructor(options: RabbitMQOptions) {
    super();
    
    logger.debug('Initializing RabbitMQ client', 'RabbitMQClient.constructor', { options });
    
    this.validateOptions(options);
    this.options = {
      heartbeat: 60,
      reconnectDelay: 5000,
      maxReconnectAttempts: -1,
      exponentialBackoff: true,
      connectionTimeout: 30000,
      failoverStrategy: 'round-robin',
      poolConfig: {
        maxChannels: 10,
        acquireTimeout: 5000,
      },
      circuitBreaker: {
        failureThreshold: 5,
        resetTimeout: 30000,
      },
      batchConfig: {
        size: 100,
        timeoutMs: 1000,
      },
      ...options,
    };

    // Initialize URLs array if single URL provided
    if (options.url) {
      const url =
        typeof options.url === 'string' ? options.url : options.url.hostname;
      this.options.urls = url ? [url] : [];
      logger.debug('Initialized URLs from single URL option', 'RabbitMQClient.constructor', { 
        urls: this.options.urls 
      });
    }

    this.initializeMetricsCollection();
    this.startNodeHealthCheck();
    
    logger.info('RabbitMQ client initialized successfully', 'RabbitMQClient.constructor', {
      heartbeat: this.options.heartbeat,
      maxChannels: this.options.poolConfig?.maxChannels,
      urls: this.options.urls?.length || 0
    });
  }

  /**
   * Initializes metrics collection and emits metrics every minute
   * @private
   * @returns {void}
   */
  private initializeMetricsCollection(): void {
    logger.debug('Starting metrics collection', 'RabbitMQClient.initializeMetricsCollection');
    
    setInterval(() => {
      logger.trace('Emitting metrics', 'RabbitMQClient.initializeMetricsCollection', this.metrics);
      this.emit('metrics', { ...this.metrics });
    }, CONSTANTS.DEFAULT_METRICS_INTERVAL);
  }

  /**
   * Validates the provided configuration options
   * 
   * @private
   * @param {RabbitMQOptions} options - Options to validate
   * @throws {Error} If options are invalid
   * @returns {void}
   */
  private validateOptions(options: RabbitMQOptions): void {
    logger.debug('Validating RabbitMQ options', 'RabbitMQClient.validateOptions', { options });

    if (options.heartbeat) {
      if (
        options.heartbeat < CONSTANTS.MIN_HEARTBEAT ||
        options.heartbeat > CONSTANTS.MAX_HEARTBEAT
      ) {
        const error = new Error(
          `Heartbeat must be between ${CONSTANTS.MIN_HEARTBEAT} and ${CONSTANTS.MAX_HEARTBEAT} seconds`,
        );
        logger.error('Invalid heartbeat configuration', 'RabbitMQClient.validateOptions', { 
          heartbeat: options.heartbeat,
          error: error.message 
        });
        throw error;
      }
    }

    if (options.reconnectDelay) {
      if (
        options.reconnectDelay < CONSTANTS.MIN_RECONNECT_DELAY ||
        options.reconnectDelay > CONSTANTS.MAX_RECONNECT_DELAY
      ) {
        const error = new Error(
          `Reconnect delay must be between ${CONSTANTS.MIN_RECONNECT_DELAY} and ${CONSTANTS.MAX_RECONNECT_DELAY} ms`,
        );
        logger.error('Invalid reconnect delay configuration', 'RabbitMQClient.validateOptions', { 
          reconnectDelay: options.reconnectDelay,
          error: error.message 
        });
        throw error;
      }
    }

    if (options.poolConfig?.maxChannels && options.poolConfig.maxChannels < 1) {
      const error = new Error('Max channels must be greater than 0');
      logger.error('Invalid pool configuration', 'RabbitMQClient.validateOptions', { 
        maxChannels: options.poolConfig.maxChannels,
        error: error.message 
      });
      throw error;
    }

    logger.debug('Options validation completed successfully', 'RabbitMQClient.validateOptions');
  }

  /**
   * Cleanup method for proper resource disposal
   * @returns {void}
   */
  public [Symbol.dispose](): void {
    logger.info('Disposing RabbitMQ client resources', 'RabbitMQClient.[Symbol.dispose]');
    void this.close();
  }

  /**
   * Acquires a channel from the pool or creates a new one if available
   * 
   * @public
   * @returns {Promise<Channel | ConfirmChannel>} Promise resolving to an available channel
   * @throws {Error} If not connected to RabbitMQ or channel acquisition times out
   * 
   * @example
   * ```typescript
   * const channel = await client.getChannel();
   * try {
   *   // Use channel for operations
   *   await channel.assertQueue('my-queue');
   * } finally {
   *   client.releaseChannel(channel);
   * }
   * ```
   */
  public async getChannel(): Promise<Channel | ConfirmChannel> {
    logger.debug('Acquiring channel from pool', 'RabbitMQClient.getChannel', {
      poolSize: this.channelPool.channels.length,
      inUse: this.channelPool.inUse.size,
      maxChannels: this.channelPool.maxChannels
    });

    if (!this.connection) {
      const error = new Error('Not connected to RabbitMQ');
      logger.error('Channel acquisition failed - no connection', 'RabbitMQClient.getChannel', { error: error.message });
      throw error;
    }

    let timeoutId: ReturnType<typeof setTimeout>;
    let checkIntervalId: ReturnType<typeof setInterval>;

    try {
      // Try to get an available channel from the pool
      const availableChannel = this.channelPool.channels.find(
        (ch) => !this.channelPool.inUse.has(ch) && this.isChannelOpen(ch),
      );

      if (availableChannel) {
        this.channelPool.inUse.add(availableChannel);
        logger.debug('Acquired existing channel from pool', 'RabbitMQClient.getChannel');
        return availableChannel;
      }

      // Create new channel if under limit
      if (this.channelPool.channels.length < this.channelPool.maxChannels) {
        logger.debug('Creating new channel for pool', 'RabbitMQClient.getChannel');
        const newChannel = await this.connection.createConfirmChannel();
        this.channelPool.channels.push(newChannel);
        this.channelPool.inUse.add(newChannel);
        logger.info('Created and acquired new channel', 'RabbitMQClient.getChannel', {
          totalChannels: this.channelPool.channels.length
        });
        return newChannel;
      }

      // Wait for a channel to become available
      logger.debug('Waiting for channel to become available', 'RabbitMQClient.getChannel', {
        timeout: this.options.poolConfig?.acquireTimeout || 5000
      });

      return new Promise((resolve, reject) => {
        timeoutId = setTimeout(() => {
          clearInterval(checkIntervalId);
          const error = new Error('Channel acquisition timeout');
          logger.error('Channel acquisition timed out', 'RabbitMQClient.getChannel', { 
            error: error.message,
            timeout: this.options.poolConfig?.acquireTimeout || 5000
          });
          reject(error);
        }, this.options.poolConfig?.acquireTimeout || 5000);

        checkIntervalId = setInterval(() => {
          const channel = this.channelPool.channels.find(
            (ch) => !this.channelPool.inUse.has(ch) && this.isChannelOpen(ch),
          );
          if (channel) {
            clearTimeout(timeoutId);
            clearInterval(checkIntervalId);
            this.channelPool.inUse.add(channel);
            logger.debug('Acquired channel after waiting', 'RabbitMQClient.getChannel');
            resolve(channel);
          }
        }, CONSTANTS.DEFAULT_CHANNEL_CHECK_INTERVAL);
      });
    } catch (error) {
      logger.error('Failed to acquire channel', 'RabbitMQClient.getChannel', { error });
      await this.handleError(error);
      throw this.ensureError(error);
    }
  }

  /**
   * Releases a channel back to the pool for reuse
   * 
   * @public
   * @param {Channel | ConfirmChannel} channel - The channel to release
   * @returns {void}
   * 
   * @example
   * ```typescript
   * const channel = await client.getChannel();
   * try {
   *   // Use channel
   * } finally {
   *   client.releaseChannel(channel);
   * }
   * ```
   */
  public releaseChannel(channel: Channel | ConfirmChannel): void {
    const wasInUse = this.channelPool.inUse.has(channel);
    this.channelPool.inUse.delete(channel);
    
    logger.debug('Released channel back to pool', 'RabbitMQClient.releaseChannel', {
      wasInUse,
      inUseCount: this.channelPool.inUse.size,
      totalChannels: this.channelPool.channels.length
    });
  }

  /**
   * Establishes a connection to RabbitMQ with automatic failover support
   * 
   * @public
   * @returns {Promise<void>} Promise that resolves when connected
   * @throws {Error} If circuit breaker is open, already reconnecting, or connection fails
   * 
   * @example
   * ```typescript
   * try {
   *   await client.connect();
   *   console.log('Connected to RabbitMQ');
   * } catch (error) {
   *   console.error('Connection failed:', error);
   * }
   * ```
   */
  public async connect(): Promise<void> {
    logger.info('Initiating RabbitMQ connection', 'RabbitMQClient.connect', {
      circuitBreakerOpen: this.circuitBreaker.isOpen,
      reconnecting: this.reconnecting,
      hasExistingPromise: !!this.connectionPromise
    });

    if (this.circuitBreaker.isOpen) {
      const error = new Error('Circuit breaker is open');
      logger.error('Connection blocked by circuit breaker', 'RabbitMQClient.connect', { 
        error: error.message,
        failures: this.circuitBreaker.failures,
        lastFailure: this.circuitBreaker.lastFailure
      });
      throw error;
    }

    if (this.connectionPromise) {
      logger.debug('Returning existing connection promise', 'RabbitMQClient.connect');
      return this.connectionPromise;
    }

    this.connectionPromise = new Promise<void>((resolve, reject) => {
      if (this.connection && this.isConnectionOpen(this.connection)) {
        logger.debug('Connection already established', 'RabbitMQClient.connect');
        resolve();
        return;
      }

      if (this.reconnecting) {
        const error = new Error('Already reconnecting.');
        logger.warn('Connection attempt blocked - already reconnecting', 'RabbitMQClient.connect', { 
          error: error.message 
        });
        reject(error);
        return;
      }

      this.reconnecting = true;
      this.emit('connecting');
      logger.debug('Starting connection establishment process', 'RabbitMQClient.connect');

      this.establishConnection(resolve, reject).catch((error: unknown) => {
        logger.error('Connection establishment failed', 'RabbitMQClient.connect', { error });
        reject(this.ensureError(error));
      });
    });

    return this.connectionPromise;
  }

  /**
   * Gets the next URL for connection attempts based on failover strategy
   * 
   * @private
   * @returns {string} Next URL to attempt connection
   * @throws {Error} If no URLs are configured or URL is invalid
   */
  private getNextUrl(): string {
    logger.debug('Getting next URL for connection', 'RabbitMQClient.getNextUrl', {
      strategy: this.options.failoverStrategy,
      currentIndex: this.currentUrlIndex,
      totalUrls: this.options.urls?.length || 0
    });

    if (!this.options.urls?.length) {
      const error = new Error('No RabbitMQ URLs configured');
      logger.error('No URLs configured for connection', 'RabbitMQClient.getNextUrl', { error: error.message });
      throw error;
    }

    const url =
      this.options.failoverStrategy === 'random'
        ? this.options.urls[
        Math.floor(Math.random() * this.options.urls.length)
        ]
        : this.options.urls[this.currentUrlIndex];

    if (!url) {
      const error = new Error('Invalid URL configuration');
      logger.error('Invalid URL retrieved from configuration', 'RabbitMQClient.getNextUrl', { 
        error: error.message,
        currentIndex: this.currentUrlIndex,
        urlsLength: this.options.urls.length
      });
      throw error;
    }

    this.currentUrlIndex =
      (this.currentUrlIndex + 1) % this.options.urls.length;
    
    logger.debug('Selected URL for connection', 'RabbitMQClient.getNextUrl', { 
      url,
      nextIndex: this.currentUrlIndex
    });
    
    return url;
  }

  /**
   * Establishes the actual connection to RabbitMQ with retry logic
   * 
   * @private
   * @param {() => void} resolve - Promise resolve function
   * @param {(error: Error) => void} reject - Promise reject function
   * @returns {Promise<void>}
   */
  private async establishConnection(
    resolve: () => void,
    reject: (error: Error) => void,
  ): Promise<void> {
    try {
      logger.info('Establishing RabbitMQ connection', 'RabbitMQClient.establishConnection', {
        reconnectAttempts: this.reconnectAttempts,
        maxAttempts: this.options.maxReconnectAttempts
      });

      const connectOptions: ExtendedConnectOptions = {
        heartbeat: this.options.heartbeat,
        vhost: this.options.vhost,
        timeout: this.options.connectionTimeout,
      };

      // Try to connect using the next URL
      let url = this.getNextUrl();
      let initialConnectionRetries = 0;

      do {
        try {
          logger.debug(`Establishing connection attempt ${initialConnectionRetries + 1} on ${url}`, 'RabbitMQClient.establishConnection');
          this.connection = await amqplib.connect(url, connectOptions) as unknown as AmqpConnection;
          logger.info('Successfully connected to RabbitMQ', 'RabbitMQClient.establishConnection', { url });
          break;
        } catch (error) {
          logger.warn(`Connection attempt ${initialConnectionRetries + 1} failed`, 'RabbitMQClient.establishConnection', {
            url,
            error: this.formatError(error),
            attempt: initialConnectionRetries + 1
          });
          initialConnectionRetries++;
          if (initialConnectionRetries < CONSTANTS.MAXIMUM_INITIAL_CONNECTION_RETRIES) {
            url = this.getNextUrl();
            logger.debug(`Trying next URL: ${url}`, 'RabbitMQClient.establishConnection');
          }
        }
      } while (initialConnectionRetries < CONSTANTS.MAXIMUM_INITIAL_CONNECTION_RETRIES)

      if (!this.connection) {
        const error = new Error(`Failed to connect after ${CONSTANTS.MAXIMUM_INITIAL_CONNECTION_RETRIES} attempts`);
        logger.error('All connection attempts failed', 'RabbitMQClient.establishConnection', { 
          error: error.message,
          attempts: initialConnectionRetries
        });
        throw error;
      }

      // Setup connection monitoring
      this.setupConnectionMonitoring();
      this.setupConnectionHandlers();
      await this.setupChannels();

      this.resetCircuitBreakerState();
      this.cleanupConnectionState();

      logger.info('Successfully established RabbitMQ connection', 'RabbitMQClient.establishConnection', {
        url,
        vhost: this.options.vhost,
        heartbeat: this.options.heartbeat
      });

      this.emit('connected');
      resolve();
    } catch (error) {
      logger.error('Failed to establish RabbitMQ connection', 'RabbitMQClient.establishConnection', {
        error: this.formatError(error),
        reconnectAttempts: this.reconnectAttempts
      });
      this.handleConnectionError(error, reject);
    }
  }

  /**
   * Sets up connection monitoring and health checks
   * 
   * @private
   * @returns {void}
   */
  private setupConnectionMonitoring(): void {
    if (!this.connection) {
      logger.warn('Cannot setup monitoring - no connection', 'RabbitMQClient.setupConnectionMonitoring');
      return;
    }

    logger.debug('Setting up connection monitoring', 'RabbitMQClient.setupConnectionMonitoring');

    // Monitor connection blocked/unblocked states
    this.connection.on('blocked', (reason: string) => {
      logger.warn('Connection blocked by broker', 'RabbitMQClient.setupConnectionMonitoring', { reason });
      this.emit('blocked', reason);
    });

    this.connection.on('unblocked', () => {
      logger.info('Connection unblocked by broker', 'RabbitMQClient.setupConnectionMonitoring');
      this.emit('unblocked');
    });

    // Periodic connection health check
    setInterval(async () => {
      try {
        const isHealthy = await this.healthCheck();
        if (!isHealthy && !this.reconnecting) {
          logger.warn('Health check failed, initiating reconnection', 'RabbitMQClient.setupConnectionMonitoring');
          await this.reconnect();
        }
      } catch (error) {
        logger.error('Health check error', 'RabbitMQClient.setupConnectionMonitoring', {
          error: this.formatError(error)
        });
      }
    }, 30000); // Every 30 seconds

    // Monitor cluster nodes health
    if (this.options.clusterOptions?.nodeRecoveryInterval) {
      setInterval(async () => {
        try {
          await this.checkClusterNodesHealth();
        } catch (error) {
          logger.error('Cluster health check error', 'RabbitMQClient.setupConnectionMonitoring', { 
            error: this.formatError(error)
          });
        }
      }, this.options.clusterOptions.nodeRecoveryInterval);
    }

    logger.debug('Connection monitoring setup completed', 'RabbitMQClient.setupConnectionMonitoring');
  }

  /**
   * Sets up connection event handlers
   * 
   * @private
   * @returns {void}
   * @throws {Error} If connection is not established
   */
  private setupConnectionHandlers(): void {
    if (!this.connection) {
      const error = new Error('Connection not established');
      logger.error('Cannot setup handlers - connection not established', 'RabbitMQClient.setupConnectionHandlers', {
        error: error.message
      });
      throw error;
    }

    logger.debug('Setting up connection event handlers', 'RabbitMQClient.setupConnectionHandlers');

    this.connection.on('error', (err: Error) => {
      logger.error(`RabbitMQ connection error: ${err.message}`, 'RabbitMQClient.setupConnectionHandlers', { 
        error: this.formatError(err),
        stack: err.stack 
      });
      this.emit('connectionError', err);
      void this.reconnect();
    });

    this.connection.on('close', () => {
      logger.warn('RabbitMQ connection closed', 'RabbitMQClient.setupConnectionHandlers', { 
        reconnecting: this.reconnecting 
      });
      this.emit('connectionClosed');
      void this.reconnect();
    });

    logger.debug('Connection event handlers setup completed', 'RabbitMQClient.setupConnectionHandlers');
  }

  /**
   * Sets up channels including default channel and channel pool
   * 
   * @private
   * @returns {Promise<void>}
   * @throws {Error} If connection is not established
   */
  private async setupChannels(): Promise<void> {
    if (!this.connection) {
      const error = new Error('Connection not established');
      logger.error('Cannot setup channels - no connection', 'RabbitMQClient.setupChannels', {
        error: error.message
      });
      throw error;
    }

    logger.debug('Setting up channels', 'RabbitMQClient.setupChannels');

    this.defaultChannel = await this.connection.createConfirmChannel();
    await this.setupDefaultChannel();
    await this.initializeChannelPool();

    // Add channel recovery logic
    setInterval(async () => {
      try {
        await this.checkAndRecoverChannels();
      } catch (error) {
        logger.error('Channel recovery error', 'RabbitMQClient.setupChannels', { 
          error: this.formatError(error)
        });
      }
    }, 5000); // Check every 5 seconds

    logger.info('Channels setup completed', 'RabbitMQClient.setupChannels', {
      defaultChannelCreated: !!this.defaultChannel,
      poolSize: this.channelPool.channels.length
    });
  }

  /**
   * Checks and recovers failed channels
   * 
   * @private
   * @returns {Promise<void>}
   */
  private async checkAndRecoverChannels(): Promise<void> {
    if (!this.connection || !this.isConnectionOpen(this.connection)) {
      logger.debug('Cannot recover channels - connection is not open', 'RabbitMQClient.checkAndRecoverChannels');
      return;
    }

    try {
      logger.trace('Checking channel health', 'RabbitMQClient.checkAndRecoverChannels');

      // Check and recover default channel
      if (!this.defaultChannel || !this.isChannelOpen(this.defaultChannel)) {
        logger.info('Attempting to recover default channel', 'RabbitMQClient.checkAndRecoverChannels');
        const maxRetries = this.options.channelOptions?.maxRetries || 3;
        const retryDelay = this.options.channelOptions?.retryDelay || 1000;

        for (let attempt = 0; attempt < maxRetries; attempt++) {
          try {
            this.defaultChannel = await this.connection.createConfirmChannel();
            await this.setupDefaultChannel();
            logger.info('Successfully recovered default channel', 'RabbitMQClient.checkAndRecoverChannels');
            break;
          } catch (error) {
            logger.error(`Failed to recover default channel, attempt ${attempt + 1}/${maxRetries}`, 'RabbitMQClient.checkAndRecoverChannels', { 
              error: this.formatError(error)
            });
            if (attempt < maxRetries - 1) {
              await new Promise((resolve) => setTimeout(resolve, retryDelay));
            } else {
              throw error;
            }
          }
        }
      }

      // Check and recover pool channels
      const recoveryPromises = this.channelPool.channels.map(
        async (_channel, index) => {
          if (!this.isChannelOpen(_channel)) {
            logger.info(`Attempting to recover pool channel ${index}`, 'RabbitMQClient.checkAndRecoverChannels');

            for (
              let attempt = 0;
              attempt < (this.options.channelOptions?.maxRetries || 3);
              attempt++
            ) {
              try {
                const newChannel = await this.recoverChannel(_channel);
                if (newChannel) {
                  this.channelPool.channels[index] = newChannel;
                  if (this.channelPool.inUse.has(_channel)) {
                    this.channelPool.inUse.delete(_channel);
                    this.channelPool.inUse.add(newChannel);
                  }
                  logger.info(`Successfully recovered pool channel ${index}`, 'RabbitMQClient.checkAndRecoverChannels');
                  break;
                }
              } catch (error) {
                logger.error(`Failed to recover pool channel ${index}, attempt ${attempt + 1}`, 'RabbitMQClient.checkAndRecoverChannels', { 
                  error: this.formatError(error)
                });
                if (
                  attempt ===
                  (this.options.channelOptions?.maxRetries || 3) - 1
                ) {
                  throw error;
                }
                await new Promise((resolve) =>
                  setTimeout(
                    resolve,
                    this.options.channelOptions?.retryDelay || 1000,
                  ),
                );
              }
            }
          }
        },
      );

      await Promise.all(recoveryPromises);
    } catch (error) {
      logger.error('Channel recovery failed, initiating reconnection', 'RabbitMQClient.checkAndRecoverChannels', { 
        error: this.formatError(error)
      });
      await this.reconnect();
    }
  }

  /**
   * Sets up the default confirm channel with prefetch and event handlers
   * 
   * @private
   * @returns {Promise<void>}
   * @throws {Error} If default channel is not established
   */
  private async setupDefaultChannel(): Promise<void> {
    if (!this.defaultChannel) {
      const error = new Error('Default channel not established');
      logger.error('Cannot setup default channel - not created', 'RabbitMQClient.setupDefaultChannel', {
        error: error.message
      });
      throw error;
    }

    logger.debug('Setting up default channel', 'RabbitMQClient.setupDefaultChannel', {
      prefetchCount: this.options.prefetchCount,
      prefetchGlobal: this.options.prefetchGlobal
    });

    if (this.options.prefetchCount) {
      await this.defaultChannel.prefetch(
        this.options.prefetchCount,
        this.options.prefetchGlobal,
      );
      logger.debug('Prefetch configured on default channel', 'RabbitMQClient.setupDefaultChannel', {
        count: this.options.prefetchCount,
        global: this.options.prefetchGlobal
      });
    }

    this.defaultChannel.on('error', async (err: Error) => {
      logger.error('Default channel error', 'RabbitMQClient.setupDefaultChannel', {
        error: this.formatError(err)
      });
      this.emit('channelError', err);
      await this.reconnect().catch((error) =>
        this.emit('error', this.ensureError(error)),
      );
    });

    this.defaultChannel.on('return', (msg) => {
      logger.warn('Message returned by broker', 'RabbitMQClient.setupDefaultChannel', {
        exchange: msg.fields.exchange,
        routingKey: msg.fields.routingKey,
        replyCode: msg.fields.replyCode,
        replyText: msg.fields.replyText
      });
      this.emit('messageReturned', msg);
    });

    this.defaultChannel.on('drain', () => {
      logger.debug('Channel drain event', 'RabbitMQClient.setupDefaultChannel');
      this.emit('channelDrain');
    });

    logger.debug('Default channel setup completed', 'RabbitMQClient.setupDefaultChannel');
  }

  /**
   * Initializes the channel pool with configured number of channels
   * 
   * @private
   * @returns {Promise<void>}
   * @throws {Error} If connection is not established
   */
  private async initializeChannelPool(): Promise<void> {
    if (!this.connection) {
      const error = new Error('Connection not established');
      logger.error('Cannot initialize channel pool - no connection', 'RabbitMQClient.initializeChannelPool', {
        error: error.message
      });
      throw error;
    }

    const maxChannels = this.options.poolConfig?.maxChannels ?? 10;
    logger.debug('Initializing channel pool', 'RabbitMQClient.initializeChannelPool', { maxChannels });

    for (let i = 0; i < maxChannels; i++) {
      try {
        const channel = await this.connection.createConfirmChannel();
        this.channelPool.channels.push(channel);
        logger.trace(`Created pool channel ${i + 1}/${maxChannels}`, 'RabbitMQClient.initializeChannelPool');
      } catch (error) {
        logger.error(`Failed to create pool channel ${i + 1}`, 'RabbitMQClient.initializeChannelPool', {
          error: this.formatError(error)
        });
        throw error;
      }
    }

    this.channelPool.maxChannels = maxChannels;
    logger.info('Channel pool initialized', 'RabbitMQClient.initializeChannelPool', {
      channelCount: this.channelPool.channels.length,
      maxChannels
    });
  }

  /**
   * Resets circuit breaker state after successful connection
   * 
   * @private
   * @returns {void}
   */
  private resetCircuitBreakerState(): void {
    const wasOpen = this.circuitBreaker.isOpen;
    this.circuitBreaker.failures = 0;
    this.circuitBreaker.isOpen = false;
    this.circuitBreaker.lastFailure = null;
    
    if (wasOpen) {
      logger.info('Circuit breaker reset after successful connection', 'RabbitMQClient.resetCircuitBreakerState');
    }
  }

  /**
   * Cleans up connection state flags
   * 
   * @private
   * @returns {void}
   */
  private cleanupConnectionState(): void {
    this.reconnecting = false;
    this.connectionPromise = null;
    logger.debug('Connection state cleaned up', 'RabbitMQClient.cleanupConnectionState');
  }

  /**
   * Handles connection errors and updates circuit breaker state
   * 
   * @private
   * @param {unknown} error - The error that occurred
   * @param {(error: Error) => void} reject - Promise rejection function
   * @returns {void}
   */
  private handleConnectionError(
    error: unknown,
    reject: (error: Error) => void,
  ): void {
    this.circuitBreaker.failures++;
    this.circuitBreaker.isOpen =
      this.circuitBreaker.failures >=
      (this.options.circuitBreaker?.failureThreshold ?? 5);
    this.circuitBreaker.lastFailure = new Date();

    logger.error('Connection error handled', 'RabbitMQClient.handleConnectionError', {
      error: this.formatError(error),
      failures: this.circuitBreaker.failures,
      circuitBreakerOpen: this.circuitBreaker.isOpen,
      threshold: this.options.circuitBreaker?.failureThreshold ?? 5
    });

    this.cleanupConnectionState();
    this.emit('connectionFailed', this.ensureError(error));
    reject(this.ensureError(error));
  }

  /**
   * Calculates the delay for the next reconnection attempt
   * 
   * @private
   * @returns {number} Delay in milliseconds
   */
  private calculateReconnectDelay(): number {
    const baseDelay = this.options.reconnectDelay || 1000;
    const maxDelay = 60000; // 1 minute max

    if (!this.options.exponentialBackoff) {
      logger.debug('Using fixed reconnect delay', 'RabbitMQClient.calculateReconnectDelay', { delay: baseDelay });
      return baseDelay;
    }

    // Calculate exponential backoff with jitter
    const exponentialDelay = Math.min(
      baseDelay * Math.pow(2, this.reconnectAttempts),
      maxDelay,
    );

    // Add random jitter (±20%)
    const jitter = exponentialDelay * 0.2 * (Math.random() * 2 - 1);
    const finalDelay = Math.max(baseDelay, Math.min(exponentialDelay + jitter, maxDelay));

    logger.debug('Calculated exponential backoff delay', 'RabbitMQClient.calculateReconnectDelay', {
      baseDelay,
      exponentialDelay,
      jitter,
      finalDelay,
      attempt: this.reconnectAttempts
    });

    return finalDelay;
  }

  /**
   * Attempts to reconnect to RabbitMQ with exponential backoff
   * 
   * @private
   * @returns {Promise<void>}
   * @throws {Error} If maximum reconnection attempts exceeded
   */
  private async reconnect(): Promise<void> {
    if (this.shutdownInProgress) {
      logger.debug('Reconnection skipped - shutdown in progress', 'RabbitMQClient.reconnect');
      return;
    }

    // Allow reconnection attempts even if reconnecting flag is true
    // as the previous attempt might have failed
    this.reconnecting = true;
    let lastError: Error | null = null;

    try {
      logger.info('Starting reconnection process', 'RabbitMQClient.reconnect', {
        attempt: this.reconnectAttempts + 1,
        maxAttempts: this.options.maxReconnectAttempts,
        shutdownInProgress: this.shutdownInProgress
      });

      // Force cleanup of existing connections/channels
      await this.forceCleanup();

      while (!this.isConnectionOpen(this.connection)) {
        try {
          const delay = this.calculateReconnectDelay();

          logger.info('Attempting to reconnect', 'RabbitMQClient.reconnect', {
            attempt: this.reconnectAttempts + 1,
            delay,
            maxAttempts: this.options.maxReconnectAttempts,
          });

          this.emit('reconnecting');

          // Wait for calculated delay
          await new Promise((resolve) => setTimeout(resolve, delay));

          // Try to establish new connection
          this.connection = await this.tryNextClusterNode();

          // Setup new connection handlers first
          this.setupConnectionHandlers();

          // Setup connection monitoring
          this.setupConnectionMonitoring();

          // Then setup channels
          await this.setupChannels();

          this.reconnectAttempts = 0;
          this.metrics.reconnections++;
          this.metrics.lastReconnectTime = new Date();

          logger.info('Successfully reconnected', 'RabbitMQClient.reconnect', {
            totalReconnections: this.metrics.reconnections,
            lastReconnectTime: this.metrics.lastReconnectTime
          });
          this.emit('reconnected');

          // Reset reconnecting flag only after successful reconnection
          this.reconnecting = false;
          return;
        } catch (error) {
          lastError = this.ensureError(error);
          this.reconnectAttempts++;

          logger.error('Reconnection attempt failed', 'RabbitMQClient.reconnect', {
            error: this.formatError(lastError),
            attempt: this.reconnectAttempts,
            maxAttempts: this.options.maxReconnectAttempts,
          });

          if (
            this.options.maxReconnectAttempts !== -1 &&
            this.reconnectAttempts >= (this.options.maxReconnectAttempts || 0)
          ) {
            logger.error('Max reconnection attempts exceeded', 'RabbitMQClient.reconnect', {
              attempts: this.reconnectAttempts,
              maxAttempts: this.options.maxReconnectAttempts,
              error: this.formatError(lastError),
            });

            this.emit('reconnectFailed', lastError);
            throw lastError;
          }
        }
      }
    } catch (error) {
      this.reconnecting = false;
      logger.error('Reconnection process failed', 'RabbitMQClient.reconnect', {
        error: this.formatError(error)
      });
      throw error;
    }
  }

  /**
   * Forces cleanup of all connections and channels
   * 
   * @private
   * @returns {Promise<void>}
   */
  private async forceCleanup(): Promise<void> {
    logger.debug('Starting force cleanup of connections and channels', 'RabbitMQClient.forceCleanup');

    try {
      // Force close all channels in the pool
      for (const channel of this.channelPool.channels) {
        try {
          if (channel && typeof channel.close === 'function') {
            await channel.close().catch(() => {
              /* ignore close errors */
            });
          }
        } catch (error) {
          logger.debug('Error closing pool channel', 'RabbitMQClient.forceCleanup', { 
            error: this.formatError(error)
          });
        }
      }
      this.channelPool.channels = [];
      this.channelPool.inUse.clear();

      // Force close default channel
      if (this.defaultChannel) {
        try {
          await this.defaultChannel.close().catch(() => {
            /* ignore close errors */
          });
        } catch (error) {
          logger.debug('Error closing default channel', 'RabbitMQClient.forceCleanup', { 
            error: this.formatError(error)
          });
        }
        this.defaultChannel = null;
      }

      // Force close connection
      if (this.connection) {
        try {
          await this.connection.close().catch(() => {
            /* ignore close errors */
          });
        } catch (error) {
          logger.debug('Error closing connection', 'RabbitMQClient.forceCleanup', { 
            error: this.formatError(error)
          });
        }
        this.connection = null;
      }

      logger.debug('Force cleanup completed', 'RabbitMQClient.forceCleanup');
    } catch (error) {
      logger.warn('Error during force cleanup', 'RabbitMQClient.forceCleanup', {
        error: this.formatError(error),
      });
    }
  }

  /**
   * Checks if a connection is open and healthy
   * 
   * @private
   * @param {Connection | null} connection - Connection to check
   * @returns {boolean} True if connection is open
   */
  private isConnectionOpen(connection: Connection | null): boolean {
    if (!connection) {
      logger.trace('Connection check: null connection', 'RabbitMQClient.isConnectionOpen');
      return false;
    }

    try {
      const conn = connection as Connection & {
        connection: { stream: { readable: boolean; writable: boolean } };
      };

      const isOpen = Boolean(
        conn.connection &&
        !(connection as unknown as { closing: boolean; closed: boolean })
          .closing &&
        !(connection as unknown as { closing: boolean; closed: boolean })
          .closed &&
        conn.connection.stream?.readable &&
        conn.connection.stream?.writable,
      );

      logger.trace('Connection status checked', 'RabbitMQClient.isConnectionOpen', { isOpen });
      return isOpen;
    } catch (error) {
      logger.debug('Error checking connection state', 'RabbitMQClient.isConnectionOpen', {
        error: this.formatError(error),
      });
      return false;
    }
  }

  /**
   * Closes the RabbitMQ client and all associated resources
   * 
   * @public
   * @returns {Promise<void>} Promise that resolves when client is closed
   * 
   * @example
   * ```typescript
   * await client.close();
   * console.log('Client closed successfully');
   * ```
   */
  public async close(): Promise<void> {
    try {
      logger.info('Initiating RabbitMQ connection shutdown', 'RabbitMQClient.close');

      this.reconnecting = false;

      if (this.messageBatch.timer) {
        clearTimeout(this.messageBatch.timer);
        this.messageBatch.timer = null;
        logger.debug('Cleared message batch timer', 'RabbitMQClient.close');
      }

      // Close all channels in the pool
      for (const channel of this.channelPool.channels) {
        try {
          if (this.isChannelOpen(channel)) {
            await channel.close();
            logger.trace('Closed pool channel', 'RabbitMQClient.close');
          }
        } catch (err) {
          logger.error('Error closing channel from pool', 'RabbitMQClient.close', { 
            error: this.formatError(err)
          });
          this.emit('channelCloseError', err);
        }
      }
      this.channelPool.channels = [];
      this.channelPool.inUse.clear();

      if (this.defaultChannel) {
        try {
          await this.defaultChannel.close();
          this.emit('channelClosed');
          logger.debug('Closed default channel', 'RabbitMQClient.close');
        } catch (err) {
          logger.error('Error closing default channel', 'RabbitMQClient.close', {
            error: this.formatError(err),
          });
          this.emit('channelCloseError', err);
        }
        this.defaultChannel = null;
      }

      if (this.connection) {
        try {
          await this.connection.close();
          this.emit('connectionClosed');
          logger.debug('Closed connection', 'RabbitMQClient.close');
        } catch (err) {
          logger.error('Error closing connection', 'RabbitMQClient.close', {
            error: this.formatError(err),
          });
          this.emit('connectionCloseError', err);
        }
        this.connection = null;
      }

      logger.info('Successfully closed RabbitMQ connection', 'RabbitMQClient.close');
      this.emit('closed');
    } catch (error) {
      logger.error('Unexpected error during connection shutdown', 'RabbitMQClient.close', { 
        error: this.formatError(error)
      });
      throw error;
    }
  }

  /**
   * Checks if a channel is open and healthy
   * 
   * @private
   * @param {Channel | ConfirmChannel | null} channel - Channel to check
   * @returns {boolean} True if channel is open
   */
  private isChannelOpen(channel: Channel | ConfirmChannel | null): boolean {
    if (!channel) {
      return false;
    }
    // Type assertion since the property exists but isn't typed
    const isOpen = !(channel as unknown as { closed: boolean }).closed;
    logger.trace('Channel status checked', 'RabbitMQClient.isChannelOpen', { isOpen });
    return isOpen;
  }

  /**
   * Ensures the default channel is available and connection is healthy
   * 
   * @private
   * @returns {void}
   * @throws {Error} If not connected or channel is not available
   */
  private ensureChannel(): void {
    if (
      !this.defaultChannel ||
      !this.connection ||
      !this.isConnectionOpen(this.connection) ||
      !this.isChannelOpen(this.defaultChannel)
    ) {
      const error = new Error('Not connected to RabbitMQ. Call connect() first.');
      logger.error('Channel check failed', 'RabbitMQClient.ensureChannel', {
        error: error.message,
        hasConnection: !!this.connection,
        hasDefaultChannel: !!this.defaultChannel,
        connectionOpen: this.connection ? this.isConnectionOpen(this.connection) : false,
        channelOpen: this.defaultChannel ? this.isChannelOpen(this.defaultChannel) : false
      });
      throw error;
    }
  }

  /**
   * Ensures error is properly typed as Error instance
   * 
   * @private
   * @param {unknown} error - The error to check
   * @returns {Error} Error instance
   */
  private ensureError(error: unknown): Error {
    return error instanceof Error ? error : new Error(String(error));
  }

  /**
   * Handles errors with logging and metrics updates
   * 
   * @private
   * @param {unknown} error - The error that occurred
   * @returns {Promise<void>}
   */
  private async handleError(error: unknown): Promise<void> {
    const err = this.ensureError(error);
    this.metrics.errors++;

    // Enhanced error logging
    logger.error(`RabbitMQ Error: ${err.message}`, 'RabbitMQClient.handleError', {
      error: this.formatError(err),
      stack: err.stack,
      metrics: this.metrics,
      connectionState: {
        isConnected: Boolean(this.connection),
        isReconnecting: this.reconnecting,
        reconnectAttempts: this.reconnectAttempts,
        circuitBreakerState: this.circuitBreaker,
      },
    });

    this.emit('error', err);

    if (!this.reconnecting) {
      logger.debug('Initiating reconnection due to error', 'RabbitMQClient.handleError');
      await this.reconnect();
    }
  }

  /**
   * Publishes a batch of messages to RabbitMQ using confirm channel
   * 
   * @public
   * @param {MessageBatch['messages']} messages - Array of messages to publish
   * @returns {Promise<void>} Promise that resolves when all messages are published
   * @throws {Error} If channel is not available or publishing fails
   * 
   * @example
   * ```typescript
   * const messages = [
   *   {
   *     exchange: 'my-exchange',
   *     routingKey: 'routing.key',
   *     content: Buffer.from('message 1'),
   *     options: {}
   *   },
   *   {
   *     exchange: 'my-exchange',
   *     routingKey: 'routing.key',
   *     content: Buffer.from('message 2'),
   *     options: {}
   *   }
   * ];
   * await client.publishBatch(messages);
   * ```
   */
  public async publishBatch(messages: MessageBatch['messages']): Promise<void> {
    logger.debug('Publishing message batch', 'RabbitMQClient.publishBatch', {
      messageCount: messages.length
    });

    this.ensureChannel();
    if (!this.defaultChannel) {
      const error = new Error('Channel not available');
      logger.error('Batch publish failed - no channel', 'RabbitMQClient.publishBatch', {
        error: error.message
      });
      throw error;
    }

    try {
      for (const msg of messages) {
        await new Promise<void>((resolve, reject) => {
          this.defaultChannel?.publish(
            msg.exchange,
            msg.routingKey,
            msg.content,
            msg.options,
            (err) => {
              if (err) {
                logger.error('Message publish failed in batch', 'RabbitMQClient.publishBatch', {
                  error: this.formatError(err),
                  exchange: msg.exchange,
                  routingKey: msg.routingKey
                });
                reject(this.ensureError(err));
              } else {
                logger.trace('Message published in batch', 'RabbitMQClient.publishBatch', {
                  exchange: msg.exchange,
                  routingKey: msg.routingKey
                });
                resolve();
              }
            },
          );
        });
      }
      this.metrics.messagesSent += messages.length;
      logger.info('Batch publish completed', 'RabbitMQClient.publishBatch', {
        messageCount: messages.length,
        totalSent: this.metrics.messagesSent
      });
    } catch (error) {
      logger.error('Batch publish failed', 'RabbitMQClient.publishBatch', {
        error: this.formatError(error),
        messageCount: messages.length
      });
      await this.handleError(error);
      throw this.ensureError(error);
    }
  }

  /**
   * Performs a health check on the RabbitMQ connection and channels
   * 
   * @public
   * @returns {Promise<boolean>} Promise resolving to true if healthy
   * 
   * @example
   * ```typescript
   * const isHealthy = await client.healthCheck();
   * if (!isHealthy) {
   *   console.log('RabbitMQ client is not healthy');
   * }
   * ```
   */
  public async healthCheck(): Promise<boolean> {
    logger.trace('Performing health check', 'RabbitMQClient.healthCheck');

    try {
      if (!this.connection || !this.isConnectionOpen(this.connection)) {
        logger.debug('Health check failed - no connection', 'RabbitMQClient.healthCheck');
        return false;
      }

      if (!this.defaultChannel || !this.isChannelOpen(this.defaultChannel)) {
        logger.debug('Health check failed - no default channel', 'RabbitMQClient.healthCheck');
        return false;
      }

      // Test channel by doing a lightweight operation
      await this.defaultChannel.assertQueue('healthCheckQueue');
      await this.defaultChannel.checkQueue('healthCheckQueue');
      await this.defaultChannel.deleteQueue('healthCheckQueue');
      
      logger.trace('Health check passed', 'RabbitMQClient.healthCheck');
      return true;
    } catch (err) {
      logger.error('Health check failed', 'RabbitMQClient.healthCheck', {
        error: this.formatError(err),
      });
      return false;
    }
  }

  /**
   * Gets current performance and operational metrics
   * 
   * @public
   * @returns {Metrics} Copy of current metrics
   * 
   * @example
   * ```typescript
   * const metrics = client.getMetrics();
   * console.log(`Messages sent: ${metrics.messagesSent}`);
   * console.log(`Messages received: ${metrics.messagesReceived}`);
   * console.log(`Errors: ${metrics.errors}`);
   * ```
   */
  public getMetrics(): Metrics {
    logger.trace('Getting current metrics', 'RabbitMQClient.getMetrics', this.metrics);
    return { ...this.metrics };
  }

  /**
   * Performs graceful shutdown of the RabbitMQ client
   * 
   * @public
   * @returns {Promise<void>} Promise that resolves when shutdown is complete
   * 
   * @example
   * ```typescript
   * await client.gracefulShutdown();
   * console.log('Client shutdown gracefully');
   * ```
   */
  public async gracefulShutdown(): Promise<void> {
    try {
      logger.info('Starting graceful shutdown...', 'RabbitMQClient.gracefulShutdown');

      if (this.shutdownInProgress) {
        logger.info('Shutdown already in progress', 'RabbitMQClient.gracefulShutdown');
        return;
      }

      this.shutdownInProgress = true;

      // Stop accepting new connections/channels immediately
      this.reconnecting = false;

      // Clear any pending timers
      if (this.messageBatch.timer) {
        clearTimeout(this.messageBatch.timer);
        this.messageBatch.timer = null;
      }

      // Wait for in-flight messages with a shorter timeout
      await this.waitForInFlightMessages(3000);

      // Close channels and connection
      logger.info('Closing channels and connection...', 'RabbitMQClient.gracefulShutdown');

      // Close all channels first
      for (const channel of this.channelPool.channels) {
        try {
          if (this.isChannelOpen(channel)) {
            await channel.close();
          }
        } catch (error) {
          logger.debug('Error closing channel', 'RabbitMQClient.gracefulShutdown', { 
            error: this.formatError(error) 
          });
        }
      }
      this.channelPool.channels = [];
      this.channelPool.inUse.clear();

      // Close default channel
      if (this.defaultChannel) {
        try {
          await this.defaultChannel.close();
        } catch (error) {
          logger.debug('Error closing default channel', 'RabbitMQClient.gracefulShutdown', { 
            error: this.formatError(error) 
          });
        }
        this.defaultChannel = null;
      }

      // Finally close the connection
      if (this.connection) {
        try {
          await this.connection.close();
          this.connection = null;
          logger.info('Connection closed successfully', 'RabbitMQClient.gracefulShutdown');
        } catch (error) {
          logger.debug('Error closing connection', 'RabbitMQClient.gracefulShutdown', { 
            error: this.formatError(error) 
          });
        }
      }

      logger.info('Graceful shutdown completed', 'RabbitMQClient.gracefulShutdown');
    } catch (error) {
      logger.error('Error during graceful shutdown', 'RabbitMQClient.gracefulShutdown', {
        error: this.formatError(error),
      });
      throw error;
    } finally {
      this.shutdownInProgress = false;
    }
  }

  /**
   * Waits for in-flight messages to complete processing
   * 
   * @private
   * @param {number} maxWaitTime - Maximum time to wait in milliseconds
   * @returns {Promise<void>}
   */
  private async waitForInFlightMessages(maxWaitTime = 5000): Promise<void> {
    const startTime = Date.now();
    logger.info('Waiting for in-flight messages to complete...', 'RabbitMQClient.waitForInFlightMessages');

    return new Promise((resolve) => {
      const checkMessages = (): void => {
        const timeElapsed = Date.now() - startTime;

        if (timeElapsed >= maxWaitTime) {
          logger.warn('Max wait time reached for in-flight messages', 'RabbitMQClient.waitForInFlightMessages');
          resolve();
          return;
        }

        // Check if there are any messages being processed
        if (this.metrics.messagesSent === this.metrics.messagesReceived) {
          logger.info('All in-flight messages completed', 'RabbitMQClient.waitForInFlightMessages');
          resolve();
          return;
        }

        // Check again after 100ms
        setTimeout(checkMessages, 100);
      };

      checkMessages();
    });
  }

  /**
   * Consumes messages from a queue with improved async handling
   * 
   * @public
   * @param {string} queue - Queue name to consume from
   * @param {(msg: Message | null) => Promise<void>} onMessage - Message handler function
   * @param {Options.Consume & { timeout?: number }} options - Consume options with optional timeout
   * @returns {Promise<string>} Promise resolving to consumer tag
   * @throws {Error} If channel is not available or consumption fails
   * 
   * @example
   * ```typescript
   * const consumerTag = await client.consume('my-queue', async (msg) => {
   *   if (msg) {
   *     console.log('Received:', msg.content.toString());
   *     // Message will be automatically acknowledged
   *   }
   * }, { noAck: false, timeout: 30000 });
   * ```
   */
  public async consume(
    queue: string,
    onMessage: (msg: Message | null) => Promise<void>,
    options: Options.Consume & { timeout?: number } = {},
  ): Promise<string> {
    logger.debug('Setting up message consumer', 'RabbitMQClient.consume', {
      queue,
      options: { ...options, timeout: options.timeout || 30000 }
    });

    this.ensureChannel();
    if (!this.defaultChannel) {
      const error = new Error('Channel not available');
      logger.error('Consumer setup failed - no channel', 'RabbitMQClient.consume', { 
        error: error.message,
        queue 
      });
      throw error;
    }

    try {
      const { consumerTag } = await this.defaultChannel.consume(
        queue,
        async (msg) => {
          try {
            const startTime = Date.now();

            // Create a timeout promise
            const timeoutPromise = new Promise<void>((_, reject) => {
              setTimeout(
                () => reject(new Error('Message processing timeout')),
                options.timeout || 30000,
              );
            });

            // Race between message processing and timeout
            await Promise.race([onMessage(msg), timeoutPromise]);

            await this.updateMetrics('received');

            const processingTime = Date.now() - startTime;
            this.metrics.avgProcessingTime =
              (this.metrics.avgProcessingTime + processingTime) / 2;

            if (msg && !options.noAck) {
              await new Promise<void>((resolve, reject) => {
                try {
                  this.defaultChannel?.ack(msg);
                  logger.trace('Message acknowledged', 'RabbitMQClient.consume', {
                    queue,
                    deliveryTag: msg.fields.deliveryTag
                  });
                  resolve();
                } catch (err) {
                  reject(
                    new Error(
                      err instanceof Error ? err.message : 'Ack failed',
                    ),
                  );
                }
              });
            }
          } catch (error) {
            logger.error('Message processing failed', 'RabbitMQClient.consume', {
              error: this.formatError(error),
              queue,
              deliveryTag: msg?.fields.deliveryTag
            });

            if (msg && !options.noAck) {
              await new Promise<void>((resolve, reject) => {
                try {
                  this.defaultChannel?.nack(msg, false, true);
                  logger.debug('Message nacked and requeued', 'RabbitMQClient.consume', {
                    queue,
                    deliveryTag: msg.fields.deliveryTag
                  });
                  resolve();
                } catch (err) {
                  reject(
                    new Error(
                      err instanceof Error ? err.message : 'Nack failed',
                    ),
                  );
                }
              });
            }
            await this.handleError(error);
          }
        },
        options,
      );

      logger.info('Consumer setup completed', 'RabbitMQClient.consume', {
        queue,
        consumerTag,
        options
      });

      return consumerTag;
    } catch (error) {
      logger.error('Failed to setup consumer', 'RabbitMQClient.consume', {
        error: this.formatError(error),
        queue
      });
      await this.handleError(error);
      throw this.ensureError(error);
    }
  }

  /**
   * Enhanced metrics collection with async capabilities
   * 
   * @public
   * @param {'sent' | 'received' | 'error' | 'reconnect'} type - Type of metric to update
   * @returns {Promise<void>}
   * 
   * @example
   * ```typescript
   * await client.updateMetrics('sent');
   * await client.updateMetrics('received');
   * ```
   */
  public async updateMetrics(
    type: 'sent' | 'received' | 'error' | 'reconnect',
  ): Promise<void> {
    try {
      switch (type) {
        case 'sent':
          this.metrics.messagesSent++;
          break;
        case 'received':
          this.metrics.messagesReceived++;
          break;
        case 'error':
          this.metrics.errors++;
          break;
        case 'reconnect':
          this.metrics.reconnections++;
          this.metrics.lastReconnectTime = new Date();
          break;
      }

      logger.trace('Metrics updated', 'RabbitMQClient.updateMetrics', {
        type,
        currentMetrics: this.metrics
      });

      // Emit metrics update event asynchronously
      await new Promise<void>((resolve) => {
        process.nextTick(() => {
          this.emit('metrics', { ...this.metrics });
          resolve();
        });
      });
    } catch (error) {
      logger.error('Failed to update metrics', 'RabbitMQClient.updateMetrics', {
        error: this.formatError(error),
        type
      });
      await this.handleError(error);
    }
  }

  /**
   * Publishes a single message with improved async handling
   * 
   * @public
   * @param {string} exchange - Exchange name
   * @param {string} routingKey - Routing key
   * @param {Buffer} content - Message content
   * @param {Options.Publish & { timeout?: number }} options - Publish options with optional timeout
   * @returns {Promise<void>} Promise that resolves when message is published
   * @throws {Error} If channel is not available or publishing fails
   * 
   * @example
   * ```typescript
   * await client.publish(
   *   'my-exchange',
   *   'routing.key',
   *   Buffer.from('Hello World'),
   *   { persistent: true, timeout: 5000 }
   * );
   * ```
   */
  public async publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: Options.Publish & { timeout?: number } = {},
  ): Promise<void> {
    logger.debug('Publishing message', 'RabbitMQClient.publish', {
      exchange,
      routingKey,
      contentLength: content.length,
      options: { ...options, timeout: options.timeout || 30000 }
    });

    this.ensureChannel();
    if (!this.defaultChannel) {
      const error = new Error('Channel not available');
      logger.error('Publish failed - no channel', 'RabbitMQClient.publish', {
        error: error.message,
        exchange,
        routingKey
      });
      throw error;
    }

    try {
      await new Promise<void>((resolve, reject) => {
        const timeoutId = setTimeout(() => {
          reject(new Error('Publish operation timeout'));
        }, options.timeout || 30000);

        this.defaultChannel?.publish(
          exchange,
          routingKey,
          content,
          options,
          (err) => {
            clearTimeout(timeoutId);
            if (err) {
              logger.error('Message publish failed', 'RabbitMQClient.publish', {
                error: this.formatError(err),
                exchange,
                routingKey
              });
              reject(this.ensureError(err));
            } else {
              logger.trace('Message published successfully', 'RabbitMQClient.publish', {
                exchange,
                routingKey,
                contentLength: content.length
              });
              resolve();
            }
          },
        );
      });

      await this.updateMetrics('sent');
    } catch (error) {
      logger.error('Failed to publish message', 'RabbitMQClient.publish', {
        error: this.formatError(error),
        exchange,
        routingKey
      });
      await this.handleError(error);
      throw this.ensureError(error);
    }
  }

  /**
   * Asserts a queue exists, creating it if necessary
   * 
   * @public
   * @param {string} queue - Queue name
   * @param {QueueOptions} options - Queue assertion options
   * @returns {Promise<amqplib.Replies.AssertQueue>} Promise resolving to queue assertion reply
   * @throws {Error} If channel is not available or assertion fails
   * 
   * @example
   * ```typescript
   * const queueInfo = await client.assertQueue('my-queue', {
   *   durable: true,
   *   deadLetterExchange: 'dlx',
   *   messageTtl: 60000
   * });
   * console.log(`Queue has ${queueInfo.messageCount} messages`);
   * ```
   */
  public async assertQueue(
    queue: string,
    options: QueueOptions = {},
  ): Promise<amqplib.Replies.AssertQueue> {
    logger.debug('Asserting queue', 'RabbitMQClient.assertQueue', {
      queue,
      options
    });

    this.ensureChannel();
    if (!this.defaultChannel) {
      const error = new Error('Channel not available');
      logger.error('Queue assertion failed - no channel', 'RabbitMQClient.assertQueue', {
        error: error.message,
        queue
      });
      throw error;
    }

    try {
      const result = await this.defaultChannel.assertQueue(queue, options);
      logger.info('Queue asserted successfully', 'RabbitMQClient.assertQueue', {
        queue,
        messageCount: result.messageCount,
        consumerCount: result.consumerCount
      });
      return result;
    } catch (error) {
      logger.error('Failed to assert queue', 'RabbitMQClient.assertQueue', {
        error: this.formatError(error),
        queue,
        options
      });
      await this.handleError(error);
      throw this.ensureError(error);
    }
  }

  /**
   * Asserts an exchange exists, creating it if necessary
   * 
   * @public
   * @param {string} exchange - Exchange name
   * @param {string} type - Exchange type (direct, topic, fanout, headers)
   * @param {ExchangeOptions} options - Exchange assertion options
   * @returns {Promise<amqplib.Replies.AssertExchange>} Promise resolving to exchange assertion reply
   * @throws {Error} If channel is not available or assertion fails
   * 
   * @example
   * ```typescript
   * await client.assertExchange('my-exchange', 'topic', {
   *   durable: true,
   *   alternateExchange: 'alternate-exchange'
   * });
   * ```
   */
  public async assertExchange(
    exchange: string,
    type: string,
    options: ExchangeOptions = {},
  ): Promise<amqplib.Replies.AssertExchange> {
    logger.debug('Asserting exchange', 'RabbitMQClient.assertExchange', {
      exchange,
      type,
      options
    });

    this.ensureChannel();
    if (!this.defaultChannel) {
      const error = new Error('Channel not available');
      logger.error('Exchange assertion failed - no channel', 'RabbitMQClient.assertExchange', {
        error: error.message,
        exchange
      });
      throw error;
    }

    try {
      const result = await this.defaultChannel.assertExchange(exchange, type, options);
      logger.info('Exchange asserted successfully', 'RabbitMQClient.assertExchange', {
        exchange,
        type
      });
      return result;
    } catch (error) {
      logger.error('Failed to assert exchange', 'RabbitMQClient.assertExchange', {
        error: this.formatError(error),
        exchange,
        type,
        options
      });
      await this.handleError(error);
      throw this.ensureError(error);
    }
  }

  /**
   * Binds a queue to an exchange with a routing pattern
   * 
   * @public
   * @param {string} queue - Queue name
   * @param {string} exchange - Exchange name
   * @param {string} pattern - Binding pattern/routing key
   * @returns {Promise<void>} Promise that resolves when binding is complete
   * @throws {Error} If channel is not available or binding fails
   * 
   * @example
   * ```typescript
   * await client.bindQueue('my-queue', 'my-exchange', 'routing.key.*');
   * ```
   */
  public async bindQueue(
    queue: string,
    exchange: string,
    pattern: string,
  ): Promise<void> {
    logger.debug('Binding queue to exchange', 'RabbitMQClient.bindQueue', {
      queue,
      exchange,
      pattern
    });

    this.ensureChannel();
    if (!this.defaultChannel) {
      const error = new Error('Channel not available');
      logger.error('Queue binding failed - no channel', 'RabbitMQClient.bindQueue', {
        error: error.message,
        queue,
        exchange
      });
      throw error;
    }

    try {
      await this.defaultChannel.bindQueue(queue, exchange, pattern);
      logger.info('Queue bound to exchange successfully', 'RabbitMQClient.bindQueue', {
        queue,
        exchange,
        pattern
      });
    } catch (error) {
      logger.error('Failed to bind queue to exchange', 'RabbitMQClient.bindQueue', {
        error: this.formatError(error),
        queue,
        exchange,
        pattern
      });
      await this.handleError(error);
      throw this.ensureError(error);
    }
  }

  /**
   * Improved cleanupStaleChannels with proper async handling
   * 
   * @public
   * @returns {Promise<void>} Promise that resolves when cleanup is complete
   * 
   * @example
   * ```typescript
   * await client.cleanupStaleChannels();
   * ```
   */
  public async cleanupStaleChannels(): Promise<void> {
    logger.debug('Starting stale channel cleanup', 'RabbitMQClient.cleanupStaleChannels');

    try {
      const staleChannels = this.channelPool.channels.filter(
        (_channel) => !this.isChannelOpen(_channel),
      );

      logger.debug('Found stale channels', 'RabbitMQClient.cleanupStaleChannels', {
        staleCount: staleChannels.length,
        totalChannels: this.channelPool.channels.length
      });

      await Promise.all(
        staleChannels.map(async (_channel) => {
          if (this.channelPool.inUse.has(_channel)) {
            this.channelPool.inUse.delete(_channel);
          }
          try {
            if (_channel && typeof _channel.close === 'function') {
              await _channel.close();
            }
          } catch (error) {
            logger.warn('Error closing stale channel', 'RabbitMQClient.cleanupStaleChannels', { 
              error: this.formatError(error) 
            });
          }
        }),
      );

      this.channelPool.channels = this.channelPool.channels.filter((_channel) =>
        this.isChannelOpen(_channel),
      );

      logger.info('Stale channel cleanup completed', 'RabbitMQClient.cleanupStaleChannels', {
        removedChannels: staleChannels.length,
        remainingChannels: this.channelPool.channels.length
      });
    } catch (error) {
      logger.error('Failed to cleanup stale channels', 'RabbitMQClient.cleanupStaleChannels', {
        error: this.formatError(error)
      });
      await this.handleError(error);
      throw this.ensureError(error);
    }
  }

  // Add new method for cluster node management
  private async tryNextClusterNode(): Promise<AmqpConnection> {
    logger.debug('Trying to connect to next cluster node', 'RabbitMQClient.tryNextClusterNode', {
      availableNodes: this.options.urls?.length || 0,
      activeNodesCount: this.activeNodes.size
    });

    if (!this.options.urls || this.options.urls.length === 0) {
      const error = new Error('No RabbitMQ cluster nodes configured');
      logger.error('No cluster nodes configured', 'RabbitMQClient.tryNextClusterNode', {
        error: error.message
      });
      throw error;
    }

    // Initialize node tracking if not done
    if (this.activeNodes.size === 0) {
      this.options.urls.forEach((url) => {
        this.activeNodes.set(url, {
          healthy: true,
          lastChecked: new Date(),
          failureCount: 0,
        });
      });
      logger.debug('Initialized node tracking', 'RabbitMQClient.tryNextClusterNode', {
        nodeCount: this.activeNodes.size
      });
    }

    const timeout = this.options.clusterOptions?.retryConnectTimeout || 5000;
    const errors: Error[] = [];

    // Get available healthy nodes
    const healthyNodes = [...this.activeNodes.entries()]
      .filter(([_, status]) => status.healthy)
      .map(([url]) => url);

    // If no healthy nodes, try all nodes
    const nodesToTry =
      healthyNodes.length > 0 ? healthyNodes : this.options.urls;

    logger.debug('Selecting nodes to try', 'RabbitMQClient.tryNextClusterNode', {
      healthyNodes: healthyNodes.length,
      totalNodes: this.options.urls.length,
      nodesToTry: nodesToTry.length
    });

    // Sort nodes based on strategy
    const sortedNodes = this.getSortedNodes(nodesToTry);

    for (const url of sortedNodes) {
      try {
        logger.debug('Attempting connection to cluster node', 'RabbitMQClient.tryNextClusterNode', { url });

        const connectOptions: ExtendedConnectOptions = {
          heartbeat: this.options.heartbeat,
          vhost: this.options.vhost,
          timeout: timeout,
        };

        const connection = await amqplib.connect(url, connectOptions) as unknown as AmqpConnection;

        // Update node status
        this.activeNodes.set(url, {
          healthy: true,
          lastChecked: new Date(),
          failureCount: 0,
        });

        logger.info('Successfully connected to cluster node', 'RabbitMQClient.tryNextClusterNode', { url });
        return connection;
      } catch (error) {
        // Update node status
        const nodeStatus = this.activeNodes.get(url) || {
          healthy: true,
          lastChecked: new Date(),
          failureCount: 0,
        };
        nodeStatus.failureCount++;
        nodeStatus.healthy = nodeStatus.failureCount < 3; // Mark unhealthy after 3 failures
        nodeStatus.lastChecked = new Date();
        this.activeNodes.set(url, nodeStatus);

        errors.push(this.ensureError(error));
        logger.warn(`Failed to connect to cluster node: ${url}`, 'RabbitMQClient.tryNextClusterNode', { 
          error: this.formatError(error), 
          nodeStatus 
        });
        continue;
      }
    }

    const aggregateError = new AggregateError(errors, 'Failed to connect to any cluster nodes');
    logger.error('All cluster nodes failed', 'RabbitMQClient.tryNextClusterNode', {
      error: aggregateError.message,
      attemptedNodes: sortedNodes.length,
      errors: errors.map(e => this.formatError(e))
    });
    throw aggregateError;
  }

  /**
   * Sorts cluster nodes based on failover strategy and priority
   * 
   * @private
   * @param {string[]} nodes - Array of node URLs to sort
   * @returns {string[]} Sorted array of node URLs
   */
  private getSortedNodes(nodes: string[]): string[] {
    const { failoverStrategy, clusterOptions } = this.options;

    logger.debug('Sorting cluster nodes', 'RabbitMQClient.getSortedNodes', {
      strategy: failoverStrategy,
      nodeCount: nodes.length,
      hasPriorityNodes: !!(clusterOptions?.priorityNodes?.length)
    });

    // First handle priority nodes
    const priorityNodes = clusterOptions?.priorityNodes || [];
    let sortedNodes = [
      ...priorityNodes.filter((node) => nodes.includes(node)),
      ...nodes.filter((node) => !priorityNodes.includes(node)),
    ];

    // Then apply failover strategy
    if (failoverStrategy === 'random') {
      sortedNodes = sortedNodes.sort(() => Math.random() - 0.5);
      logger.debug('Applied random sorting to nodes', 'RabbitMQClient.getSortedNodes');
    } else if (failoverStrategy === 'round-robin') {
      // Rotate array based on currentUrlIndex
      const rotateAmount = this.currentUrlIndex % sortedNodes.length;
      sortedNodes = [
        ...sortedNodes.slice(rotateAmount),
        ...sortedNodes.slice(0, rotateAmount),
      ];
      this.currentUrlIndex++;
      logger.debug('Applied round-robin sorting to nodes', 'RabbitMQClient.getSortedNodes', {
        rotateAmount,
        newIndex: this.currentUrlIndex
      });
    }

    return sortedNodes;
  }

  /**
   * Starts periodic health checks for cluster nodes
   * 
   * @private
   * @returns {void}
   */
  private startNodeHealthCheck(): void {
    const interval = this.options.clusterOptions?.nodeRecoveryInterval || 30000;

    logger.debug('Starting node health check', 'RabbitMQClient.startNodeHealthCheck', {
      interval,
      hasClusterOptions: !!this.options.clusterOptions
    });

    setInterval(async () => {
      try {
        await this.checkClusterNodesHealth();
      } catch (error) {
        logger.error('Node health check interval error', 'RabbitMQClient.startNodeHealthCheck', {
          error: this.formatError(error)
        });
      }
    }, interval);
  }

  /**
   * Recovers a failed channel by creating a new one
   * 
   * @private
   * @param {Channel | ConfirmChannel} _channel - The failed channel to recover
   * @returns {Promise<Channel | ConfirmChannel | null>} Promise resolving to new channel or null
   */
  private async recoverChannel(
    _channel: Channel | ConfirmChannel,
  ): Promise<Channel | ConfirmChannel | null> {
    logger.debug('Attempting to recover channel', 'RabbitMQClient.recoverChannel');

    if (!this.connection || !this.isConnectionOpen(this.connection)) {
      logger.debug('Cannot recover channel - no connection', 'RabbitMQClient.recoverChannel');
      return null;
    }

    try {
      const newChannel = await this.connection.createConfirmChannel();

      // Restore channel settings
      if (this.options.prefetchCount) {
        await newChannel.prefetch(
          this.options.prefetchCount,
          this.options.prefetchGlobal,
        );
        logger.debug('Restored prefetch settings on recovered channel', 'RabbitMQClient.recoverChannel', {
          prefetchCount: this.options.prefetchCount,
          prefetchGlobal: this.options.prefetchGlobal
        });
      }

      // Setup channel event handlers
      newChannel.on('error', async (err: Error) => {
        logger.error('Recovered channel error', 'RabbitMQClient.recoverChannel', {
          error: this.formatError(err)
        });
        this.emit('channelError', err);
        await this.handleChannelError(newChannel, err);
      });

      newChannel.on('close', () => {
        logger.debug('Recovered channel closed', 'RabbitMQClient.recoverChannel');
        this.emit('channelClosed');
      });

      logger.info('Channel recovered successfully', 'RabbitMQClient.recoverChannel');
      return newChannel;
    } catch (error) {
      logger.error('Failed to recover channel', 'RabbitMQClient.recoverChannel', { 
        error: this.formatError(error) 
      });
      return null;
    }
  }

  /**
   * Handles channel-specific errors and recovery
   * 
   * @private
   * @param {Channel | ConfirmChannel} channel - The channel that encountered an error
   * @param {Error} _error - The error that occurred
   * @returns {Promise<void>}
   */
  private async handleChannelError(
    channel: Channel | ConfirmChannel,
    _error: Error,
  ): Promise<void> {
    logger.error('Channel error occurred', 'RabbitMQClient.handleChannelError', {
      error: this.formatError(_error)
    });

    // Remove from pool if it's a pool channel
    const poolIndex = this.channelPool.channels.indexOf(channel);
    if (poolIndex !== -1) {
      this.channelPool.channels.splice(poolIndex, 1);
      this.channelPool.inUse.delete(channel);
      logger.debug('Removed failed channel from pool', 'RabbitMQClient.handleChannelError', {
        poolIndex,
        remainingChannels: this.channelPool.channels.length
      });
    }

    // Try to recover the channel
    const recoveredChannel = await this.recoverChannel(channel);
    if (recoveredChannel) {
      if (poolIndex !== -1) {
        this.channelPool.channels.push(recoveredChannel);
        logger.debug('Added recovered channel to pool', 'RabbitMQClient.handleChannelError');
      } else if (this.defaultChannel === channel) {
        this.defaultChannel = recoveredChannel as ConfirmChannel;
        logger.debug('Replaced default channel with recovered channel', 'RabbitMQClient.handleChannelError');
      }
    }
  }

  /**
   * Checks the health of all cluster nodes
   * 
   * @private
   * @returns {Promise<void>}
   */
  private async checkClusterNodesHealth(): Promise<void> {
    if (!this.options.urls) {
      logger.debug('No URLs configured for cluster health check', 'RabbitMQClient.checkClusterNodesHealth');
      return;
    }

    logger.trace('Starting cluster nodes health check', 'RabbitMQClient.checkClusterNodesHealth', {
      nodeCount: this.options.urls.length
    });

    for (const url of this.options.urls) {
      try {
        const connectOptions: ExtendedConnectOptions = {
          heartbeat: this.options.heartbeat,
          vhost: this.options.vhost,
          timeout: 5000,
        };

        const testConnection = await amqplib.connect(url, connectOptions);
        await testConnection.close();

        this.activeNodes.set(url, {
          healthy: true,
          lastChecked: new Date(),
          failureCount: 0,
        });

        logger.trace('Cluster node health check passed', 'RabbitMQClient.checkClusterNodesHealth', { url });
      } catch (error) {
        const status = this.activeNodes.get(url) || {
          healthy: true,
          lastChecked: new Date(),
          failureCount: 0,
        };
        status.failureCount++;
        status.healthy = status.failureCount < 3;
        status.lastChecked = new Date();
        this.activeNodes.set(url, status);

        logger.warn('Cluster node health check failed', 'RabbitMQClient.checkClusterNodesHealth', {
          url,
          error: this.formatError(error),
          status,
        });
      }
    }
  }

  /**
   * Formats error objects into readable strings
   * 
   * @private
   * @param {unknown} error - The error to format
   * @returns {string} Formatted error string
   */
  private formatError(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    } else if (typeof error === 'string') {
      return error;
    } else {
      return 'Unknown error format';
    }
  }
}

export default RabbitMQClient;