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
var EuiIconCompute = function EuiIconCompute(_ref) {
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
    d: "M4 3a1 1 0 00-1 1v8a1 1 0 001 1h8a1 1 0 001-1V4a1 1 0 00-1-1H4zm10 8v1a2 2 0 01-2 2h-1v2h-1v-2H8.5v2h-1v-2H6v2H5v-2H4a2 2 0 01-2-2v-1H0v-1h2V8.5H0v-1h2V6H0V5h2V4a2 2 0 012-2h1V0h1v2h1.5V0h1v2H10V0h1v2h1a2 2 0 012 2v1h2v1h-2v1.5h2v1h-2V10h2v1h-2z"
  }), (0, _react2.jsx)("rect", {
    width: 6,
    height: 6,
    x: 5,
    y: 5,
    rx: 1
  }));
};

var icon = EuiIconCompute;
exports.icon = icon;