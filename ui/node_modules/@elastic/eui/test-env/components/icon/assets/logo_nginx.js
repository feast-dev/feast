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
var EuiIconLogoNginx = function EuiIconLogoNginx(_ref) {
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
  }, title) : null, (0, _react2.jsx)("g", {
    fill: "none",
    fillRule: "evenodd"
  }, (0, _react2.jsx)("path", {
    fill: "#119639",
    d: "M16 0l13.856 8v16L16 32 2.144 24V8z"
  }), (0, _react2.jsx)("path", {
    fill: "#FFF",
    fillRule: "nonzero",
    d: "M11.17 13.512v8.376a1.607 1.607 0 11-3.215 0V9.632c0-1.432 1.731-2.149 2.744-1.136l9.51 9.512V9.632a1.607 1.607 0 013.215 0v12.256c0 1.432-1.731 2.149-2.744 1.136l-9.51-9.512z"
  })));
};

var icon = EuiIconLogoNginx;
exports.icon = icon;