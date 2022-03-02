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
var EuiIconLogoAppSearch = function EuiIconLogoAppSearch(_ref) {
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
    fill: "#0080D5",
    d: "M19.5.938a7.002 7.002 0 00-7 0l-8 4.619A7 7 0 001 11.62v9.237a7 7 0 003.5 6.062l7.5 4.33V17.979a7 7 0 013.5-6.062L27 5.276 19.5.939z"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillNegative",
    d: "M19.5.938a7.002 7.002 0 00-7 0L5 5.277l11 6.35 11-6.35-7.5-4.34z"
  }), (0, _react2.jsx)("path", {
    fill: "#FA744E",
    d: "M28.435 7.76l-10.026 5.79a6.994 6.994 0 011.59 4.428v13.27l7.5-4.33a7 7 0 003.5-6.061v-9.238a6.992 6.992 0 00-1.586-4.422l-.978.564z"
  }));
};

var icon = EuiIconLogoAppSearch;
exports.icon = icon;