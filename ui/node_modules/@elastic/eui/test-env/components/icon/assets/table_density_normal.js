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
var EuiIconTableDensityNormal = function EuiIconTableDensityNormal(_ref) {
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
    d: "M16 3v11a2 2 0 01-2 2H2a2 2 0 01-2-2V2a2 2 0 012-2h12a2 2 0 012 2v1zm-1 0V2a1 1 0 00-1-1H2a1 1 0 00-1 1v1h14zm0 1H1v10a1 1 0 001 1h12a1 1 0 001-1V4zM4.5 6a.5.5 0 010 1H2.496a.5.5 0 110-1H4.5zm5.214 0c.158 0 .286.224.286.5s-.128.5-.286.5H6.286C6.128 7 6 6.776 6 6.5s.128-.5.286-.5h3.428zM4.5 9a.5.5 0 010 1H2.496a.5.5 0 110-1H4.5zm5.214 0c.158 0 .286.224.286.5s-.128.5-.286.5H6.286C6.128 10 6 9.776 6 9.5s.128-.5.286-.5h3.428zM4.5 12a.5.5 0 110 1H2.496a.5.5 0 110-1H4.5zm9-6a.5.5 0 110 1h-2.004a.5.5 0 010-1H13.5zm0 3a.5.5 0 110 1h-2.004a.5.5 0 010-1H13.5zm0 3a.5.5 0 110 1h-2.004a.5.5 0 010-1H13.5zm-3.786 0c.158 0 .286.224.286.5s-.128.5-.286.5H6.286C6.128 13 6 12.776 6 12.5s.128-.5.286-.5h3.428z"
  }));
};

var icon = EuiIconTableDensityNormal;
exports.icon = icon;