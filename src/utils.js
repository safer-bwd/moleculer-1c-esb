const cloneDeep = require('lodash.clonedeep');
const merge = require('lodash.merge');
const pick = require('lodash.pick');
const tinyAsyncPool = require('tiny-async-pool');

const asyncPool = async (...args) => {
  const results = [];

  /* eslint no-restricted-syntax: 0 */
  for await (const result of tinyAsyncPool(...args)) {
    results.push(result);
  }

  return results;
};

const isString = (v) => typeof v === 'string' || v instanceof String;

const isArray = (v) => Array.isArray(v);

const noop = () => {};

module.exports = {
  asyncPool,
  cloneDeep,
  isArray,
  isString,
  merge,
  noop,
  pick,
};
