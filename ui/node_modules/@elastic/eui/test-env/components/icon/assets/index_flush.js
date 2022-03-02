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
var EuiIconIndexFlush = function EuiIconIndexFlush(_ref) {
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
    fillRule: "evenodd",
    d: "M14.516 9H10.5a.5.5 0 010-1h4.016L13.11 5.948c-.171-.252-.137-.62.079-.821.217-.2.531-.159.703.092l2 2.916a.648.648 0 01.108.397.643.643 0 01-.108.332l-2 2.918A.478.478 0 0113.5 12a.457.457 0 01-.312-.127c-.216-.202-.25-.57-.079-.82L14.516 9zM3 15H1a1 1 0 01-1-1V2a1 1 0 011-1h7.8c.274 0 .537.113.726.312l2.2 2.428c.176.186.274.433.274.689V7h-1V5H8.5a.5.5 0 01-.5-.5V2H3v12h8v-4h1v4a1 1 0 01-1 1H3zm-1-1V2H1v12h1z"
  }));
};

var icon = EuiIconIndexFlush;
exports.icon = icon;