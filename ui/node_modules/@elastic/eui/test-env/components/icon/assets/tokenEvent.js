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
var EuiIconTokenEvent = function EuiIconTokenEvent(_ref) {
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
    d: "M11.225 5.656c0 .423-.106.79-.318 1.102-.211.311-.51.57-.898.775a5.435 5.435 0 01-1.392.485c-.54.117-1.14.193-1.798.229a6.047 6.047 0 00-.035.67c0 .258.02.51.062.757.04.247.114.464.22.652s.25.34.432.458.414.176.696.176c.211 0 .467-.044.766-.132.3-.088.62-.244.96-.467.106-.07.192-.129.256-.176a.365.365 0 01.22-.07c.118 0 .197.061.238.185a.99.99 0 01.062.255 7.1 7.1 0 01-.573.467 4.93 4.93 0 01-.775.467c-.288.141-.6.261-.934.361-.335.1-.678.15-1.03.15-.541 0-.982-.088-1.322-.264a2.072 2.072 0 01-.793-.688 2.626 2.626 0 01-.388-.933 4.949 4.949 0 01-.106-1.005c0-.634.103-1.257.309-1.868.205-.61.499-1.157.88-1.638.383-.482.838-.87 1.366-1.163A3.567 3.567 0 019.093 4c.599 0 1.104.126 1.515.379.411.252.617.678.617 1.277zm-2.467-.951c-.223 0-.435.08-.635.238-.2.158-.381.373-.546.643-.164.27-.305.578-.423.925a6.42 6.42 0 00-.264 1.101c.47-.047.863-.135 1.18-.264.318-.13.57-.285.758-.467.188-.182.323-.388.405-.617.083-.229.124-.467.124-.713 0-.27-.056-.479-.168-.626a.519.519 0 00-.431-.22z"
  }));
};

var icon = EuiIconTokenEvent;
exports.icon = icon;