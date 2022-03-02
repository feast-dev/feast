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
var EuiIconCloudStormy = function EuiIconCloudStormy(_ref) {
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
    d: "M7.421 4.93a.5.5 0 11-.87.49 3 3 0 10-4.43 3.918.5.5 0 01-.626.78 4 4 0 013.973-6.84l.032.018V3.28a5.5 5.5 0 117.003 7.357.5.5 0 11-.36-.934 4.5 4.5 0 10-5.77-5.923c.42.31.778.701 1.05 1.15h-.002zM9.6 11c.669.002.794.67.36 1.003l-4.68 3.882c-.457.378-1.053-.26-.643-.689l3.08-3.193A5411.7 5411.7 0 015.113 12c-.668-.001-.793-.669-.36-1.003l4.68-3.881c.458-.379 1.053.26.643.688l-3.08 3.193L9.6 11z"
  }));
};

var icon = EuiIconCloudStormy;
exports.icon = icon;