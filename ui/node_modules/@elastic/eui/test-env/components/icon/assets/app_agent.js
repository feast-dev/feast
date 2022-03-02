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
var EuiIconAppAgent = function EuiIconAppAgent(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = (0, _objectWithoutProperties2.default)(_ref, ["title", "titleId"]);
  return (0, _react2.jsx)("svg", (0, _extends2.default)({
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    xmlns: "http://www.w3.org/2000/svg",
    "aria-labelledby": titleId
  }, props), title ? (0, _react2.jsx)("title", {
    id: titleId
  }, title) : null, (0, _react2.jsx)("path", {
    className: "euiIcon__fillSecondary",
    d: "M21 2.82L16 .038 11 2.82v2.289l5-2.782 5 2.782v-2.29z"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillSecondary",
    d: "M21 7.282L16 4.5l-5 2.782V9.57l5-2.781 5 2.781V7.282z"
  }), (0, _react2.jsx)("path", {
    d: "M7 5.045L2 7.827v15.577l14 7.788 14-7.788V7.827l-5-2.782v2.289l3 1.669v13.225l-12 6.676-12-6.676V9.003l3-1.669V5.045z"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillSecondary",
    fillRule: "evenodd",
    clipRule: "evenodd",
    d: "M22 12.5L16 9l-6 3.5v7l6 3.5 6-3.5v-7zm-9.974 1.205L16 11.387l3.974 2.318v4.59L16 20.613l-3.974-2.318v-4.59z"
  }));
};

var icon = EuiIconAppAgent;
exports.icon = icon;