/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import _times from 'lodash/times';
import _memoize from 'lodash/memoize'; // wrap the lodash functions to avoid having lodash's TS type definition from being
// exported, which can conflict with the lodash namespace if other versions are used

export function times(count, iteratee) {
  if (iteratee === undefined) {
    return _times(count);
  }

  return _times(count, iteratee);
}
export function memoize(func, resolver) {
  return _memoize(func, resolver);
}
export var browserTick = function browserTick(callback) {
  requestAnimationFrame(callback);
};