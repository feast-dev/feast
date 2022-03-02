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
var EuiIconTokenJoin = function EuiIconTokenJoin(_ref) {
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
    clipRule: "evenodd",
    d: "M7.5 4.5v1.025c0 1.269-1.185 1.908-2.112 1.737a.75.75 0 100 1.475c.927-.17 2.112.47 2.112 1.739v1.023h4v-1.005a2.5 2.5 0 010-4.988V4.5h-4zM13 4a1 1 0 00-1-1H7a1 1 0 00-1 1v1.525c0 .172-.172.293-.341.262a2.25 2.25 0 100 4.426c.17-.031.341.09.341.262V12a1 1 0 001 1h5a1 1 0 001-1V9.004a.16.16 0 00-.04-.105c-.109-.125-.594-.16-.732-.068a1 1 0 110-1.662c.138.092.623.057.732-.068a.16.16 0 00.04-.105V4z"
  }));
};

var icon = EuiIconTokenJoin;
exports.icon = icon;