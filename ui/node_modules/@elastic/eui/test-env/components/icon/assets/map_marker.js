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
var EuiIconMapMarker = function EuiIconMapMarker(_ref) {
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
    d: "M7.55 14.224a.502.502 0 00.897 0l4-8a.5.5 0 00.053-.235 4.672 4.672 0 00-.084-.705 5.538 5.538 0 00-.505-1.512C11.189 2.362 9.906 1.5 8 1.5c-1.906 0-3.19.862-3.91 2.272-.248.485-.41.998-.506 1.512-.058.31-.08.554-.084.705a.5.5 0 00.053.235l3.997 8zM8 5a1 1 0 100 2 1 1 0 000-2zm-3.493.895c.009-.106.027-.253.06-.429.079-.424.213-.848.413-1.238C5.537 3.138 6.487 2.5 8 2.5c1.513 0 2.463.638 3.02 1.728.2.39.334.814.413 1.238.033.176.051.323.06.43L8 12.881 4.507 5.895z"
  }));
};

var icon = EuiIconMapMarker;
exports.icon = icon;