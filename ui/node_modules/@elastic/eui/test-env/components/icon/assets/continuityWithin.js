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
var EuiIconContinuityWithin = function EuiIconContinuityWithin(_ref) {
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
    d: "M.5 3a.5.5 0 00-.5.5v9a.5.5 0 001 0v-9A.5.5 0 00.5 3zm14.5.5a.5.5 0 011 0v9a.5.5 0 01-1 0v-9zm-4.712 1.547a.5.5 0 01.532.069l3 2.5a.5.5 0 010 .768l-3 2.5A.5.5 0 0110 10.5V9H6v1.5a.5.5 0 01-.82.384l-3-2.5a.5.5 0 010-.768l3-2.5A.5.5 0 016 5.5V7h4V5.5a.5.5 0 01.288-.453z"
  }));
};

var icon = EuiIconContinuityWithin;
exports.icon = icon;