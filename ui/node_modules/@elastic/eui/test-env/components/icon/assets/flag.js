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
var EuiIconFlag = function EuiIconFlag(_ref) {
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
    d: "M7.686 8.464c1.547-.619 3.08-.619 4.628 0A.5.5 0 0013 8V2a.5.5 0 00-.276-.447C11.259.82 9.458.82 7.342 1.526c-1.884.628-3.417.628-4.618.027A.5.5 0 002 2v12.5a.5.5 0 101 0V8.553c1.411.627 2.983.592 4.686-.089zM3 2.741c1.322.42 2.878.327 4.658-.267C9.4 1.894 10.843 1.85 12 2.322v4.975c-1.56-.464-3.128-.384-4.686.239-1.54.616-2.892.616-4.09.017A.498.498 0 003 7.5V2.74z"
  }));
};

var icon = EuiIconFlag;
exports.icon = icon;