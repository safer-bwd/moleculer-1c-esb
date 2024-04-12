const get = require('lodash.get');
const merge = require('lodash.merge');
const pick = require('lodash.pick');

const isString = (v) => typeof v === 'string' || v instanceof String;

const isArray = (v) => Array.isArray(v);

const noop = () => {};

module.exports = {
  get,
  isArray,
  isString,
  merge,
  noop,
  pick,
};
