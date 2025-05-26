/**
 * @fileoverview Advanced logging utility with OpenTelemetry tracing, colorized output, and file rotation
 * @module logger
 */

/**
 * @typedef {Object} Logger
 * @property {function(message: string, functionName: string, meta?: object): void} fatal - Log fatal error messages
 * @property {function(message: string, functionName: string, meta?: object): void} error - Log error messages
 * @property {function(message: string, functionName: string, meta?: object): void} warn - Log warning messages
 * @property {function(message: string, functionName: string, meta?: object): void} info - Log info messages
 * @property {function(message: string, functionName: string, meta?: object): void} debug - Log debug messages
 * @property {function(message: string, functionName: string, meta?: object): void} trace - Log trace messages
 */
import os from 'os';

import { trace, context } from '@opentelemetry/api';
import chalk from 'chalk';
import { createLogger, format, transports, Logger } from 'winston';

import 'winston-daily-rotate-file';
import type { TransformableInfo } from 'logform';

const { combine, timestamp, printf, errors, json } = format;

const traceIdFormat = format((info) => {
  const span = trace.getSpan(context.active());
  if (span) {
    const spanContext = span.spanContext();
    info.traceId = spanContext.traceId;
    info.spanId = spanContext.spanId;
    span.addEvent('log', {
      level: info.level,
      message: String(info.message),
      traceId: info.traceId as string,
      spanId: info.spanId as string,
    });
  }
  return info;
})();

const environmentInfo = format((info) => {
  info.env = process.env.ENV || 'LOCAL';
  info.service = process.env.SERVICE_NAME || 'noon-parser-service';
  info.hostname = os.hostname();
  info.version = process.env.SERVICE_VERSION || '0.0.0';
  return info;
})();

// Remove the functionNameFormat as we'll now accept the function name directly

const colorizeLevel = (level: string): string => {
  switch (level.toLowerCase()) {
    case 'fatal':
      return chalk.bgRed.white(level);
    case 'error':
      return chalk.red(level);
    case 'warn':
      return chalk.yellow(level);
    case 'info':
      return chalk.green(level);
    case 'debug':
      return chalk.blue(level);
    case 'trace':
      return chalk.gray(level);
    default:
      return level;
  }
};

const customFormat = printf((info: TransformableInfo): string => {
  const {
    level,
    message,
    timestamp,
    traceId,
    spanId,
    env,
    service,
    hostname,
    functionName,
    version,
    // ...rest
  } = info;
  // const metadata = Object.keys(rest).length ? JSON.stringify(rest) : '';
  return `[${chalk.bold.underline.grey(timestamp)}] [${chalk.dim.cyan(service)}] [${chalk.dim.magenta(env)}] [${chalk.dim.yellow(hostname)}] [${chalk.dim.blue(version)}] [${chalk.dim.green(functionName || 'N/A')}] [${chalk.dim.italic.white.underline(traceId || 'N/A')}] [${chalk.dim.italic.white.underline(spanId || 'N/A')}] [${colorizeLevel(level)}]:\n ${chalk.bold.blueBright(String(message))}
  `.trim();
});

const logLevel: string = (process.env.SYSTEM_LOG_LEVEL || 'debug').toLowerCase();

const fileRotateTransport = new transports.DailyRotateFile({
  filename: 'logs/%DATE%-combined.log',
  datePattern: 'YYYY-MM-DD',
  maxSize: '50m',
  maxFiles: '14d',
  format: combine(timestamp(), json()),
  zippedArchive: true,
});

const errorFileRotateTransport = new transports.DailyRotateFile({
  filename: 'logs/%DATE%-error.log',
  datePattern: 'YYYY-MM-DD',
  maxSize: '50m',
  maxFiles: '14d',
  level: 'error',
  format: combine(timestamp(), json()),
  zippedArchive: true,
});

const requestIdFormat = format((info) => {
  const reqId = context.active().getValue(Symbol.for('requestId'));
  if (reqId) {
    info.requestId = reqId;
  }
  return info;
})();

const logger: Logger = createLogger({
  level: logLevel,
  format: combine(
    timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
    errors({ stack: true }),
    traceIdFormat,
    requestIdFormat,
    environmentInfo,
    customFormat,
  ),
  defaultMeta: { service: process.env.SERVICE_NAME || 'noon-parser-service' },
  transports: [
    new transports.Console({
      format: combine(
        format.colorize(),
        timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
        customFormat,
      ),
    }),
    fileRotateTransport,
    errorFileRotateTransport,
  ],
  exceptionHandlers: [new transports.File({ filename: 'logs/exceptions.log' })],
  rejectionHandlers: [new transports.File({ filename: 'logs/rejections.log' })],
});

// Wrap the logger methods to accept a function name
const wrappedLogger = {
  fatal: (message: string, functionName: string, meta?: object): Logger =>
    logger.error(message, { level: 'fatal', functionName, ...meta }),
  error: (message: string, functionName: string, meta?: object): Logger =>
    logger.error(message, { functionName, ...meta }),
  warn: (message: string, functionName: string, meta?: object): Logger =>
    logger.warn(message, { functionName, ...meta }),
  info: (message: string, functionName: string, meta?: object): Logger =>
    logger.info(message, { functionName, ...meta }),
  debug: (message: string, functionName: string, meta?: object): Logger =>
    logger.debug(message, { functionName, ...meta }),
  trace: (message: string, functionName: string, meta?: object): Logger =>
    logger.debug(message, { level: 'trace', functionName, ...meta }),
};

// Ensure all uncaught exceptions are logged before the process exits
// process.on('uncaughtException', (error) => {
//   wrappedLogger.error(error.message, 'UncaughtExceptionHandler', { error });
//   setTimeout(() => process.exit(1), 1000);
// });

// process.on('unhandledRejection', (reason, promise) => {
//   wrappedLogger.error('Unhandled Rejection', 'UnhandledRejectionHandler', {
//     reason,
//     promise,
//   });
// });

// Graceful shutdown
process.on('SIGTERM', () => {
  wrappedLogger.info(
    'SIGTERM received. Shutting down gracefully.',
    'GracefulShutdown',
  );
  // Perform cleanup operations here
  setTimeout(() => process.exit(0), 1000);
});

export default wrappedLogger;