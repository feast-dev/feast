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
var EuiIconLockOpen = function EuiIconLockOpen(_ref) {
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
    d: "M11.143 5c.473 0 .857.448.857 1v9c0 .552-.384 1-.857 1H.857C.384 16 0 15.552 0 15V6c0-.552.384-1 .857-1H8v-.8C8 1.88 9.79 0 12 0s4 1.88 4 4.2V5h-1v-.64C15 2.504 13.657 1 12 1S9 2.504 9 4.36V5h2.143zM1 15h10V6H1v9zm5.998-3.706L7.5 12.5h-3l.502-1.206A1.644 1.644 0 014.5 10.1c0-.883.672-1.6 1.5-1.6s1.5.717 1.5 1.6c0 .475-.194.901-.502 1.194z"
  }));
};

var icon = EuiIconLockOpen;
exports.icon = icon;