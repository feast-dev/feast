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
var EuiIconAppIndexRollup = function EuiIconAppIndexRollup(_ref) {
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
    d: "M32 26v-2h-2.1a5 5 0 00-.73-1.75l1.49-1.49-1.41-1.41-1.49 1.49A5 5 0 0026 20.1V18h-2v2.1a5 5 0 00-1.75.73l-1.49-1.49-1.41 1.41 1.49 1.49A5 5 0 0020.1 24H18v2h2.1a5 5 0 00.73 1.75l-1.49 1.49 1.41 1.41 1.49-1.49a5 5 0 001.76.74V32h2v-2.1a5 5 0 001.75-.73l1.49 1.49 1.41-1.41-1.49-1.49A5 5 0 0029.9 26H32zm-7 2a3 3 0 110-6 3 3 0 010 6z"
  }), (0, _react2.jsx)("path", {
    d: "M25.71 24.29a1 1 0 00-1.09-.21 1.15 1.15 0 00-.33.21.93.93 0 00-.21.33 1 1 0 001.3 1.3 1.15 1.15 0 00.33-.21 1 1 0 00.21-1.09.94.94 0 00-.21-.33z"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillSecondary",
    d: "M5 6h16v2H5zM5 12h16v2H5zM5 18h10v2H5zM5 24h8v2H5z"
  }), (0, _react2.jsx)("path", {
    d: "M16 32H3a3 3 0 01-3-3V3a3 3 0 013-3h20a3 3 0 013 3v13h-2V3a1 1 0 00-1-1H3a1 1 0 00-1 1v26a1 1 0 001 1h13v2z"
  }));
};

var icon = EuiIconAppIndexRollup;
exports.icon = icon;