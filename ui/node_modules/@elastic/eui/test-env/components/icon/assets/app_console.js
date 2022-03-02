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
var EuiIconAppConsole = function EuiIconAppConsole(_ref) {
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
    d: "M29 9H3a3 3 0 01-3-3V3a3 3 0 013-3h26a3 3 0 013 3v3a3 3 0 01-3 3zM3 2a1 1 0 00-1 1v3a1 1 0 001 1h26a1 1 0 001-1V3a1 1 0 00-1-1H3z"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillSecondary",
    d: "M29 32H3a3 3 0 01-3-3V14a3 3 0 013-3h26a3 3 0 013 3v15a3 3 0 01-3 3zM3 13a1 1 0 00-1 1v15a1 1 0 001 1h26a1 1 0 001-1V14a1 1 0 00-1-1H3z"
  }), (0, _react2.jsx)("path", {
    d: "M7.29 17.71l3.3 3.29-3.3 3.29 1.42 1.42 4.7-4.71-4.7-4.71zM15 24h9v2h-9z"
  }));
};

var icon = EuiIconAppConsole;
exports.icon = icon;