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
var EuiIconLogoPrometheus = function EuiIconLogoPrometheus(_ref) {
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
    fill: "#E6522C",
    d: "M15.907.187C7.122.187 0 7.308 0 16.093S7.122 32 15.907 32c8.784 0 15.906-7.121 15.906-15.906S24.69.187 15.907.187zm0 29.77c-2.5 0-4.526-1.67-4.526-3.729h9.051c0 2.06-2.026 3.73-4.525 3.73zm7.475-4.963H8.43v-2.711h14.95v2.71zm-.054-4.107H8.473c-.05-.057-.1-.113-.147-.17-1.53-1.859-1.891-2.829-2.241-3.818-.006-.032 1.855.38 3.176.678 0 0 .679.157 1.672.338-.953-1.118-1.52-2.539-1.52-3.991 0-3.189 2.446-5.975 1.564-8.227.858.07 1.777 1.812 1.839 4.537.913-1.262 1.295-3.566 1.295-4.978 0-1.463.963-3.161 1.927-3.22-.86 1.417.223 2.63 1.184 5.642.361 1.132.315 3.035.594 4.243.092-2.508.523-6.167 2.114-7.43-.702 1.591.104 3.581.655 4.538.889 1.544 1.428 2.714 1.428 4.926 0 1.483-.548 2.88-1.472 3.971 1.05-.197 1.776-.374 1.776-.374l3.411-.666s-.495 2.038-2.4 4.001z"
  }));
};

var icon = EuiIconLogoPrometheus;
exports.icon = icon;