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
var EuiIconMlCreatePopulationJob = function EuiIconMlCreatePopulationJob(_ref) {
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
    d: "M16 0c5.523 0 10 4.477 10 10s-4.477 10-10 10S6 15.523 6 10 10.477 0 16 0zm0 2a8 8 0 100 16 8 8 0 000-16z"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillSecondary",
    d: "M8.4 19.288c.492.402 1.015.766 1.566 1.087L7.26 23.682a4 4 0 11-1.437-1.243l2.578-3.151zm15.2 0l2.577 3.15a4 4 0 11-1.437 1.243l-2.706-3.306a12.032 12.032 0 001.565-1.087zM4 24a2 2 0 100 4 2 2 0 000-4zm24 0a2 2 0 100 4 2 2 0 000-4zM17 6v3h3v2h-3v3h-2v-3h-3V9h3V6h2z"
  }));
};

var icon = EuiIconMlCreatePopulationJob;
exports.icon = icon;