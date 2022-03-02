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
var EuiIconImport = function EuiIconImport(_ref) {
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
    d: "M9 10.114l1.85-1.943a.52.52 0 01.77 0c.214.228.214.6 0 .829l-1.95 2.05a1.552 1.552 0 01-2.31 0L5.41 9a.617.617 0 010-.829.52.52 0 01.77 0L8 10.082V1.556C8 1.249 8.224 1 8.5 1s.5.249.5.556v8.558zM4.18 6a.993.993 0 00-.972.804l-1.189 6A.995.995 0 002.991 14h11.018a1 1 0 00.972-1.196l-1.19-6a.993.993 0 00-.97-.804H4.18zM6 5v1h5V5h1.825c.946 0 1.76.673 1.946 1.608l1.19 6A2 2 0 0114.016 15H2.984a1.992 1.992 0 01-1.945-2.392l1.19-6C2.414 5.673 3.229 5 4.174 5H6z"
  }));
};

var icon = EuiIconImport;
exports.icon = icon;