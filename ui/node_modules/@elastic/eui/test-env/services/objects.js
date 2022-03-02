"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.omit = exports.get = void 0;

var _get2 = _interopRequireDefault(require("lodash/get"));

var _omit2 = _interopRequireDefault(require("lodash/omit"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// wrap the lodash functions to avoid having lodash's TS type definition from being
// exported, which can conflict with the lodash namespace if other versions are used
var get = function get(object, path, defaultValue) {
  return (0, _get2.default)(object, path, defaultValue);
};

exports.get = get;

var omit = function omit(object, paths) {
  return (0, _omit2.default)(object, paths);
};

exports.omit = omit;