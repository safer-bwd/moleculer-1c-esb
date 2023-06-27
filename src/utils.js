const { backOff } = require('exponential-backoff');
const get = require('lodash.get');
const merge = require('lodash.merge');
const pick = require('lodash.pick');

const isString = (str) => typeof str === 'string' || str instanceof String;

module.exports = {
  backOff,
  get,
  isString,
  merge,
  pick,
};
