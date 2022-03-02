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
var EuiIconBrush = function EuiIconBrush(_ref) {
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
    d: "M11.993 8.17c0 .83-.673 1.507-1.499 1.507H5.498A1.505 1.505 0 013.999 8.17V6.662h7.994V8.17zm-2.998 5.998c0 .455-.448.827-.999.827-.55 0-1-.372-1-.827v-3.486h2v3.486zM4 5.658V1.005h1.262v4.653H4zm2.261 0V1.005h1.244v4.653H6.26zm2.244 0V1.005h1.235v4.653H8.504zm2.234 0V1.005h1.254v4.653h-1.254zM3.008 0L3 8.17a2.509 2.509 0 002.498 2.512h.5v3.486c0 1.01.896 1.832 1.998 1.832 1.102 0 1.998-.822 1.998-1.832v-3.486h.5a2.509 2.509 0 002.498-2.512L13 0H3.008z"
  }));
};

var icon = EuiIconBrush;
exports.icon = icon;