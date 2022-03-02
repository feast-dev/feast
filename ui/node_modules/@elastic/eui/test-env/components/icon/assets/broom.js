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
var EuiIconBroom = function EuiIconBroom(_ref) {
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
    d: "M3.732 13.096l-.197-.197 2.83-2.828.706.707-2.829 2.828.708.708 2.828-2.828a1 1 0 000-1.414L5.658 7.95a.993.993 0 00-.708-.293.994.994 0 00-.708.293l-2.828 2.829.707.707 2.829-2.83.707.708-2.829 2.829.904.903zm1.218-6.44c.512 0 1.023.196 1.414.587l2.121 2.12a2 2 0 010 2.83L4.95 15.728 0 10.778l3.535-3.535a1.993 1.993 0 011.415-.586zM14.02 1l.708.707-6.95 6.95-.707-.707L14.021 1z"
  }));
};

var icon = EuiIconBroom;
exports.icon = icon;