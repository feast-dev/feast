"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.icon = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var React = _interopRequireWildcard(require("react"));

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// THIS IS A GENERATED FILE. DO NOT MODIFY MANUALLY. @see scripts/compile-icons.js
var EuiIconEyeClosed = function EuiIconEyeClosed(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = (0, _objectWithoutProperties2.default)(_ref, ["title", "titleId"]);
  return (0, _react2.jsx)("svg", (0, _extends2.default)({
    xmlns: "http://www.w3.org/2000/svg",
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    "aria-labelledby": titleId
  }, props), title ? (0, _react2.jsx)("title", {
    id: titleId
  }, title) : null, (0, _react2.jsx)("path", {
    d: "M5.318 13.47l.776-.776A6.04 6.04 0 008 13c1.999 0 3.74-.956 5.225-2.587A12.097 12.097 0 0014.926 8a12.097 12.097 0 00-1.701-2.413l-.011-.012.707-.708c1.359 1.476 2.045 2.976 2.058 3.006.014.03.021.064.021.098v.06a.24.24 0 01-.02.097C15.952 8.188 13.291 14 8 14a7.03 7.03 0 01-2.682-.53zM2.04 11.092C.707 9.629.034 8.158.02 8.128A.24.24 0 010 8.03v-.059c0-.034.007-.068.02-.098C.048 7.813 2.709 2 8 2c.962 0 1.837.192 2.625.507l-.78.78A6.039 6.039 0 008 3c-2 0-3.74.956-5.225 2.587a12.098 12.098 0 00-1.701 2.414 12.11 12.11 0 001.674 2.383l-.708.708zM8.362 4.77L7.255 5.877a2.262 2.262 0 00-1.378 1.378L4.77 8.362A3.252 3.252 0 018.362 4.77zm2.86 2.797a3.254 3.254 0 01-3.654 3.654l1.06-1.06a2.262 2.262 0 001.533-1.533l1.06-1.06zm-9.368 7.287a.5.5 0 01-.708-.708l13-13a.5.5 0 01.708.708l-13 13z"
  }));
};

var icon = EuiIconEyeClosed;
exports.icon = icon;