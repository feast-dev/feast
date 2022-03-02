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
var EuiIconPagesSelect = function EuiIconPagesSelect(_ref) {
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
    fillRule: "evenodd",
    d: "M3 1a1 1 0 011-1h8a1 1 0 01.707.293l2 2A1 1 0 0115 3v5a4.995 4.995 0 00-1-.584V4h-2a1 1 0 01-1-1V1H4v12h3.1c.07.348.177.682.316 1H4a1 1 0 01-1-1V1zm5 14H2V2a1 1 0 00-1 1v12a1 1 0 001 1h7a5.029 5.029 0 01-1-1zm8-3a4 4 0 11-8 0 4 4 0 018 0zm-1.646-1.354a.5.5 0 010 .708l-2.5 2.5a.5.5 0 01-.708 0l-1-1a.5.5 0 01.708-.708l.646.647 2.146-2.147a.5.5 0 01.708 0z"
  }));
};

var icon = EuiIconPagesSelect;
exports.icon = icon;