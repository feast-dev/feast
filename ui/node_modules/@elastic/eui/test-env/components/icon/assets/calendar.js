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
var EuiIconCalendar = function EuiIconCalendar(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = (0, _objectWithoutProperties2.default)(_ref, ["title", "titleId"]);
  return (0, _react2.jsx)("svg", (0, _extends2.default)({
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    xmlns: "http://www.w3.org/2000/svg",
    "aria-labelledby": titleId
  }, props), title ? (0, _react2.jsx)("title", {
    id: titleId
  }, title) : null, (0, _react2.jsx)("path", {
    d: "M14 4v-.994C14 2.45 13.55 2 12.994 2H11v1h-1V2H6v1H5V2H3.006C2.45 2 2 2.45 2 3.006v9.988C2 13.55 2.45 14 3.006 14h9.988C13.55 14 14 13.55 14 12.994V5H2V4h12zm-3-3h1.994C14.102 1 15 1.897 15 3.006v9.988A2.005 2.005 0 0112.994 15H3.006A2.005 2.005 0 011 12.994V3.006C1 1.898 1.897 1 3.006 1H5V0h1v1h4V0h1v1zM4 7h2v1H4V7zm3 0h2v1H7V7zm3 0h2v1h-2V7zM4 9h2v1H4V9zm3 0h2v1H7V9zm3 0h2v1h-2V9zm-6 2h2v1H4v-1zm3 0h2v1H7v-1zm3 0h2v1h-2v-1z",
    fillRule: "evenodd"
  }));
};

var icon = EuiIconCalendar;
exports.icon = icon;