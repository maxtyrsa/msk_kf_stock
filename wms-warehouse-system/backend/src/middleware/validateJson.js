import Joi from 'joi';

// Schema for order items from Python PDF parser
const orderItemSchema = Joi.object({
  product: Joi.string().allow('', null),
  article: Joi.string().allow('', null),
  model: Joi.string().allow('', null),
  quantity: Joi.number().integer().min(1).default(1)
});

// Schema for a single order
const orderSchema = Joi.object({
  order_number: Joi.string().required(),
  items: Joi.array().items(orderItemSchema).min(1).required()
});

// Schema for batch import (array of orders)
const importBatchSchema = Joi.array().items(orderSchema).min(1);

/**
 * Validate JSON import data from Python PDF parser
 * Expected format:
 * [
 *   {
 *     "order_number": "12345",
 *     "items": [
 *       {
 *         "product": "Product Name",
 *         "article": "ART-001",
 *         "model": "Model X",
 *         "quantity": 2
 *       }
 *     ]
 *   }
 * ]
 */
export const validateImportJson = (data) => {
  return importBatchSchema.validate(data, {
    abortEarly: false,
    stripUnknown: true
  });
};

/**
 * Validate single order data
 */
export const validateOrder = (data) => {
  return orderSchema.validate(data, {
    abortEarly: false,
    stripUnknown: true
  });
};

/**
 * Validate product search query
 */
export const validateSearchQuery = Joi.object({
  q: Joi.string().min(1).max(200).required(),
  page: Joi.number().integer().min(1).default(1),
  limit: Joi.number().integer().min(1).max(100).default(20)
});

/**
 * Validate stock adjustment
 */
export const validateStockAdjustment = Joi.object({
  product_id: Joi.number().integer().required(),
  quantity_change: Joi.number().integer().required(),
  transaction_type: Joi.string().valid('stock_in', 'stock_out', 'adjustment', 'audit').required(),
  notes: Joi.string().max(500).allow('', null)
});

/**
 * Validate manual product creation
 */
export const validateProductCreate = Joi.object({
  name: Joi.string().min(1).max(500).required(),
  model: Joi.string().max(200).allow('', null),
  article: Joi.string().min(1).max(200).required(),
  quantity: Joi.number().integer().min(0).default(0)
});
