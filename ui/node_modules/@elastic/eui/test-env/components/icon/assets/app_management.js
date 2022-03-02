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
var EuiIconAppManagement = function EuiIconAppManagement(_ref) {
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
    className: "euiIcon__fillSecondary",
    d: "M16 21a5 5 0 110-10 5 5 0 010 10zm0-8a3 3 0 100 6 3 3 0 000-6z"
  }), (0, _react2.jsx)("path", {
    d: "M20 32h-8v-4.06a1 1 0 00-1.61-.67l-2.88 2.87-5.65-5.65 2.87-2.87a.92.92 0 00.2-1 .93.93 0 00-.86-.6H0V12h4.06a.92.92 0 00.85-.58.94.94 0 00-.19-1L1.86 7.51l5.65-5.65 2.87 2.87A1 1 0 0012 4.06V0h8v4.06a1 1 0 001.61.67l2.87-2.87 5.66 5.66-2.87 2.87a.92.92 0 00-.2 1 .93.93 0 00.86.6H32v8h-4.06a.92.92 0 00-.85.58.94.94 0 00.19 1l2.87 2.87-5.66 5.66-2.87-2.87a1 1 0 00-1.61.67L20 32zm-6-2h4v-2.06a3 3 0 015-2.08l1.46 1.46 2.83-2.83L25.86 23a3 3 0 012.08-5H30v-4h-2.06a3 3 0 01-2.08-5l1.46-1.46-2.83-2.85L23 6.14a3 3 0 01-5-2.08V2h-4v2.06a3 3 0 01-5 2.08L7.51 4.69 4.69 7.51 6.14 9a3 3 0 01-2.08 5H2v4h2.06a3 3 0 012.08 5l-1.45 1.49 2.83 2.83L9 25.86a3 3 0 015 2.08V30z"
  }));
};

var icon = EuiIconAppManagement;
exports.icon = icon;