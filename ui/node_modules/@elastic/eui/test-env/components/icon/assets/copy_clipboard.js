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
var EuiIconCopyClipboard = function EuiIconCopyClipboard(_ref) {
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
    d: "M2 2.729V2a1 1 0 011-1h2v1H3v12h4v1H3a1 1 0 01-1-1V2.729zM14 5V2a1 1 0 00-1-1h-2v1h2v3h1zm-1 1h2v9H8V6h5V5H8a1 1 0 00-1 1v9a1 1 0 001 1h7a1 1 0 001-1V6a1 1 0 00-1-1h-2v1z"
  }), (0, _react2.jsx)("path", {
    d: "M9 10h5V9H9v1zm0-2h5V7H9v1zm0 4h5v-1H9v1zm0 2h5v-1H9v1zm2-12V1a1 1 0 00-1-1H6a1 1 0 00-1 1v1h1V1h4v1h1zM5 3h6V2H5v1z"
  }));
};

var icon = EuiIconCopyClipboard;
exports.icon = icon;