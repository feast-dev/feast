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
var EuiIconLogoWorkplaceSearch = function EuiIconLogoWorkplaceSearch(_ref) {
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
    d: "M17.557 5.05c.17-.202.457-.512.86-.93C19.483 3.017 21.828 1 23.801 1H30v30h-6.2c-1.972 0-3.995-1.449-5.383-3.106l-.764-1.052 2.75-3.477c3.462-4.376 3.463-10.332.001-14.707L17.557 5.05z",
    fill: "#FEC514"
  }), (0, _react2.jsx)("path", {
    d: "M2 31V1h5.81c1.71 0 3.35.786 4.387 2.1l5.855 7.42c2.598 3.283 2.598 7.698-.002 10.983l-5.855 7.403C11.158 30.217 9.52 31 7.815 31H2z",
    fill: "#F04E98"
  }), (0, _react2.jsx)("path", {
    d: "M15.384 24.89L12.15 21.7c-2.868-3.318-2.868-8.22.002-11.322 1.535-1.639 2.611-2.718 3.23-3.238l2.67 3.238c2.637 3.385 2.575 7.987-.062 11.374l-2.607 3.137z",
    className: "euiIcon__fillNegative"
  }));
};

var icon = EuiIconLogoWorkplaceSearch;
exports.icon = icon;