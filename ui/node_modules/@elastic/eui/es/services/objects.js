/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import _get from 'lodash/get';
import _omit from 'lodash/omit'; // wrap the lodash functions to avoid having lodash's TS type definition from being
// exported, which can conflict with the lodash namespace if other versions are used

export var get = function get(object, path, defaultValue) {
  return _get(object, path, defaultValue);
};
export var omit = function omit(object, paths) {
  return _omit(object, paths);
};