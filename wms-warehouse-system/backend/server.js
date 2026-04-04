import app from './src/app.js';
import logger from './src/utils/logger.js';

// Graceful shutdown handling
process.on('SIGTERM', () => {
  logger.info('SIGTERM signal received: closing HTTP server');
  process.exit(0);
});

process.on('SIGINT', () => {
  logger.info('SIGINT signal received: closing HTTP server');
  process.exit(0);
});

process.on('uncaughtException', (error) => {
  logger.error({ error }, 'Uncaught Exception');
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error({ reason }, 'Unhandled Rejection at');
  process.exit(1);
});

logger.info('WMS Backend Server starting...');
