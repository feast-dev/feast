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
var EuiIconAppHeartbeat = function EuiIconAppHeartbeat(_ref) {
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
    d: "M4.14 15.39a8.058 8.058 0 01-2.2-4.043A6.788 6.788 0 014.198 5.47a6.73 6.73 0 018.727-.213l1.26-1.464a8.65 8.65 0 00-11.277.232A8.727 8.727 0 00.068 11.6a10.172 10.172 0 002.793 5.275l1.28-1.484z"
  }), (0, _react2.jsx)("path", {
    d: "M15.515 31.274L4.548 18.454 15.855 4.763a8.67 8.67 0 0112.266-.746 8.727 8.727 0 012.91 7.205c-.243 2.695-2.037 4.732-3.482 6.37L15.515 31.275zm-8.427-12.82l8.427 9.862 10.55-11.995c1.32-1.503 2.822-3.21 3.007-5.265a6.788 6.788 0 00-2.24-5.586 6.73 6.73 0 00-9.504.563L7.088 18.455z"
  }));
};

var icon = EuiIconAppHeartbeat;
exports.icon = icon;