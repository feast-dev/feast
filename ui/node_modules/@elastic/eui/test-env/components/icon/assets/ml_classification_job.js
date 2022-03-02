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
var EuiIconMlClassificationJob = function EuiIconMlClassificationJob(_ref) {
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
    d: "M7 16v5h2.038a13.179 13.179 0 000 2H7v5H5v-5H0v-2h5v-5h2zM7 0v5h5v2H7v5H5V7H0V5h5V0h2zm16 0v5h5v2h-5v2.038a13.179 13.179 0 00-2 0V7h-5V5h5V0h2z"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillSecondary",
    d: "M22 10c3.073 0 5.877 1.155 8 3.056v3.252A9.82 9.82 0 1016.307 30h-3.251A11.955 11.955 0 0110 22c0-6.627 5.373-12 12-12zm1 8v3h3v2h-3v3h-2v-3h-3v-2h3v-3h2z"
  }));
};

var icon = EuiIconMlClassificationJob;
exports.icon = icon;