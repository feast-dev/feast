"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isObject = exports.isNaN = exports.isNumber = exports.isBoolean = exports.isString = exports.isArray = exports.isFunction = void 0;

var _isFunction2 = _interopRequireDefault(require("lodash/isFunction"));

var _isArray2 = _interopRequireDefault(require("lodash/isArray"));

var _isString2 = _interopRequireDefault(require("lodash/isString"));

var _isBoolean2 = _interopRequireDefault(require("lodash/isBoolean"));

var _isNumber2 = _interopRequireDefault(require("lodash/isNumber"));

var _isNaN2 = _interopRequireDefault(require("lodash/isNaN"));

var _isObject2 = _interopRequireDefault(require("lodash/isObject"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// wrap the lodash functions to avoid having lodash's TS type definition from being
// exported, which can conflict with the lodash namespace if other versions are used
var isFunction = function isFunction(value) {
  return (0, _isFunction2.default)(value);
};

exports.isFunction = isFunction;

var isArray = function isArray(value) {
  return (0, _isArray2.default)(value);
};

exports.isArray = isArray;

var isString = function isString(value) {
  return (0, _isString2.default)(value);
};

exports.isString = isString;

var isBoolean = function isBoolean(value) {
  return (0, _isBoolean2.default)(value);
};

exports.isBoolean = isBoolean;

var isNumber = function isNumber(value) {
  return (0, _isNumber2.default)(value);
};

exports.isNumber = isNumber;

var isNaN = function isNaN(value) {
  return (0, _isNaN2.default)(value);
};

exports.isNaN = isNaN;

var isObject = function isObject(value) {
  return (0, _isObject2.default)(value);
};

exports.isObject = isObject;