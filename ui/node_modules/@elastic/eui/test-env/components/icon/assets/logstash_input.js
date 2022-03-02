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
var EuiIconLogstashInput = function EuiIconLogstashInput(_ref) {
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
    d: "M1.747 10.992h13.1a.123.123 0 00.122-.123V8.51a.122.122 0 00-.122-.122H1.122A.122.122 0 001 8.51v2.36c0 .066.055.122.122.122h.625zm12.011 1H2.21V16h-1v-4.008h-.088A1.124 1.124 0 010 10.87V8.51c0-.62.503-1.122 1.122-1.122h13.725c.62 0 1.122.502 1.122 1.122v2.36c0 .618-.503 1.122-1.122 1.122h-.089V16h-1v-4.008zm-6.27-7.487V0h1v4.529l2.407-2.262.685.73L8 6.356 4.42 2.995l.685-.729 2.383 2.24z"
  }));
};

var icon = EuiIconLogstashInput;
exports.icon = icon;