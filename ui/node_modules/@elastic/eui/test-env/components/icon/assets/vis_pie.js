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
var EuiIconVisPie = function EuiIconVisPie(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = (0, _objectWithoutProperties2.default)(_ref, ["title", "titleId"]);
  return (0, _react2.jsx)("svg", (0, _extends2.default)({
    xmlns: "http://www.w3.org/2000/svg",
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    "aria-labelledby": titleId
  }, props), title ? (0, _react2.jsx)("title", {
    id: titleId
  }, title) : null, (0, _react2.jsx)("path", {
    d: "M6.5 9a.5.5 0 01-.5-.5V3.023A5.5 5.5 0 1011.978 9H6.5zM7 8h5.5a.5.5 0 01.5.5A6.5 6.5 0 116.5 2a.5.5 0 01.5.5V8zm2-6.972V6h4.972C13.696 3.552 11.448 1.304 9 1.028zM14.5 7h-6a.5.5 0 01-.5-.5v-6a.5.5 0 01.5-.5C11.853 0 15 3.147 15 6.5a.5.5 0 01-.5.5zM6.146 8.854a.5.5 0 11.708-.708l4 4a.5.5 0 01-.708.708l-4-4z"
  }));
};

var icon = EuiIconVisPie;
exports.icon = icon;