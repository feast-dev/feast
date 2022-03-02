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
var EuiIconLogstashQueue = function EuiIconLogstashQueue(_ref) {
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
    d: "M11.339 15.464H4.77a3.248 3.248 0 01-3.245-3.244V4.549H0v-1h2.526v8.67a2.247 2.247 0 002.245 2.245h6.568a2.247 2.247 0 002.244-2.244V3.549h2.455v1h-1.455v7.67a3.247 3.247 0 01-3.244 3.245zm.513-5.962v1.095l-3.848 1.72-3.85-1.72V9.502l3.85 1.72 3.848-1.72zm0-4.251v1.095l-3.848 1.72-3.85-1.72V5.25l3.85 1.72 3.848-1.72zm0-4.251v1.095l-3.848 1.72-3.85-1.72V1l3.85 1.72L11.852 1z"
  }));
};

var icon = EuiIconLogstashQueue;
exports.icon = icon;