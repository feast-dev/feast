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
var EuiIconLogoCloudEce = function EuiIconLogoCloudEce(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = (0, _objectWithoutProperties2.default)(_ref, ["title", "titleId"]);
  return (0, _react2.jsx)("svg", (0, _extends2.default)({
    xmlns: "http://www.w3.org/2000/svg",
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    "aria-labelledby": titleId
  }, props), title ? (0, _react2.jsx)("title", {
    id: titleId
  }, title) : null, (0, _react2.jsx)("path", {
    className: "euiIcon__fillNegative",
    d: "M18 0v10a6 6 0 00-5.53 8.33c-.034.009-.068.012-.1.023A18.947 18.947 0 003.975 23.7 15.934 15.934 0 012 16C2 7.164 9.163 0 18 0zm0 13a3 3 0 110 6 3 3 0 010-6z"
  }), (0, _react2.jsx)("path", {
    fill: "#00AEFA",
    d: "M22.742 21.218c-.71-.22-1.478-.135-2.146.188A5.947 5.947 0 0118 22a5.94 5.94 0 01-2.596-.594c-.669-.323-1.436-.408-2.146-.188a16.006 16.006 0 00-7.54 5.032A15.959 15.959 0 0018 32c4.936 0 9.348-2.236 12.283-5.75a16.016 16.016 0 00-7.54-5.032"
  }), (0, _react2.jsx)("path", {
    fill: "#0080D5",
    d: "M18 0A15.959 15.959 0 005.717 5.75a16.006 16.006 0 007.541 5.032c.71.22 1.477.135 2.146-.188A5.94 5.94 0 0118 10a5.94 5.94 0 012.596.594c.669.323 1.436.408 2.146.188a16.01 16.01 0 007.541-5.032A15.959 15.959 0 0018 0"
  }));
};

var icon = EuiIconLogoCloudEce;
exports.icon = icon;