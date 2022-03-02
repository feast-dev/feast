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
var EuiIconLogoMetrics = function EuiIconLogoMetrics(_ref) {
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
  }, title) : null, (0, _react2.jsx)("g", {
    fill: "none",
    fillRule: "evenodd"
  }, (0, _react2.jsx)("path", {
    fill: "#F04E98",
    d: "M2 32h28V20l-6.465-6.465a5 5 0 00-7.07 0L2 28v4z"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillNegative",
    d: "M16.465 13.535l-3.536 3.536a9.965 9.965 0 007.07 2.93 9.965 9.965 0 007.072-2.93l-3.536-3.536a5 5 0 00-7.07 0"
  }), (0, _react2.jsx)("path", {
    fill: "#FEC514",
    d: "M14.343 11.414A7.951 7.951 0 0120 9.071c2.137 0 4.146.832 5.657 2.343l3.207 3.207A9.955 9.955 0 0030 10.001c0-5.524-4.477-10-10-10-5.522 0-10 4.476-10 10 0 1.667.414 3.237 1.137 4.62l3.206-3.207z"
  })));
};

var icon = EuiIconLogoMetrics;
exports.icon = icon;