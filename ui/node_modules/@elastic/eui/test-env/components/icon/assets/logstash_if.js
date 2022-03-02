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
var EuiIconLogstashIf = function EuiIconLogstashIf(_ref) {
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
    d: "M6.918 9.799l-4.82 4.819 2.88-.43.148.987L0 15.943l.768-5.126.988.148-.453 3.033 4.936-4.935a2.003 2.003 0 011.265-2.885V0h1v6.178a2.003 2.003 0 011.263 2.884l4.865 4.866-.443-2.963.988-.147.768 5.126-5.127-.769.15-.988 2.95.441-4.83-4.83c-.312.203-.685.32-1.085.32-.4 0-.773-.117-1.085-.32z"
  }));
};

var icon = EuiIconLogstashIf;
exports.icon = icon;