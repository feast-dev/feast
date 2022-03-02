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
var EuiIconAppPipeline = function EuiIconAppPipeline(_ref) {
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
    d: "M29 12a3 3 0 00-3 3h-4a3 3 0 00-3-3h-6a3 3 0 00-3 3H6a3 3 0 00-3-3H0v14h3a3 3 0 003-3h4a3 3 0 003 3h6a3 3 0 003-3h4a3 3 0 003 3h3V12h-3zM3 24H2V14h1a1 1 0 011 1v8a1 1 0 01-1 1zm17-3v2a1 1 0 01-1 1h-6a1 1 0 01-1-1v-2H6v-4h6v-2a1 1 0 011-1h6a1 1 0 011 1v2h6v4h-6zm10 3h-1a1 1 0 01-1-1v-8a1 1 0 011-1h1v10z"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillSecondary",
    d: "M22 6H10v2h5v2h2V8h5z"
  }));
};

var icon = EuiIconAppPipeline;
exports.icon = icon;