import pino from 'pino';
import dotenv from 'dotenv';

dotenv.config();

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: {
    target: 'pino-pretty',
    options: {
      colorize: true,
      translateTime: 'SYS:standard',
      ignore: 'pid,hostname'
    }
  },
  formatters: {
    level: (label) => ({ level: label.toUpperCase() })
  }
});

// Create child loggers for different modules
export const httpLogger = logger.child({ module: 'http' });
export const dbLogger = logger.child({ module: 'database' });
export const importLogger = logger.child({ module: 'import' });
export const productLogger = logger.child({ module: 'products' });
export const orderLogger = logger.child({ module: 'orders' });
export const analyticsLogger = logger.child({ module: 'analytics' });

export default logger;
