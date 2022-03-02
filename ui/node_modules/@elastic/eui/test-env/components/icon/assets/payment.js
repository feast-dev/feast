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
var EuiIconPayment = function EuiIconPayment(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = (0, _objectWithoutProperties2.default)(_ref, ["title", "titleId"]);
  return (0, _react2.jsx)("svg", (0, _extends2.default)({
    xmlns: "http://www.w3.org/2000/svg",
    width: 16,
    height: 16,
    fill: "none",
    viewBox: "0 0 16 16",
    "aria-labelledby": titleId
  }, props), title ? (0, _react2.jsx)("title", {
    id: titleId
  }, title) : null, (0, _react2.jsx)("path", {
    d: "M.586 2.586A2 2 0 000 4h1a1 1 0 011-1V2a2 2 0 00-1.414.586zM2 2h10.5a.5.5 0 010 1H2V2zM0 4h1v6.5a.5.5 0 01-1 0V4zm2.586.586A2 2 0 002 6h1a1 1 0 011-1V4a2 2 0 00-1.414.586zm0 8.828A2 2 0 012 12h1a1 1 0 001 1v1a2 2 0 01-1.414-.586zm12.828-8.828A2 2 0 0116 6h-1a1 1 0 00-1-1V4a2 2 0 011.414.586zm0 8.828A2 2 0 0016 12h-1a1 1 0 01-1 1v1a2 2 0 001.414-.586zM4 4h10v1H4zM3 7h12v1H3zm1 6h10v1H4zM2 6h1v6H2zm13 0h1v6h-1zm-5.5 4a.5.5 0 010 1H7.496a.5.5 0 010-1H9.5zm4 0a.5.5 0 010 1h-2.004a.5.5 0 010-1H13.5z"
  }));
};

var icon = EuiIconPayment;
exports.icon = icon;