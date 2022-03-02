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
var EuiIconVisGoal = function EuiIconVisGoal(_ref) {
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
    d: "M10.725 3.653a6 6 0 012.847 7.576.5.5 0 01-.928-.372 5 5 0 10-9.293-.014.5.5 0 01-.218.619L1.39 12.47a.5.5 0 01-.708-.23A7.99 7.99 0 010 9a8 8 0 0111.212-7.329.5.5 0 01.234.704l-.721 1.278zm-.933-.38l.5-.889a7 7 0 00-8.902 8.93l.886-.511a6 6 0 017.516-7.53zM6.73 9.467a1.75 1.75 0 112.539 0 2 2 0 11-2.539 0zM8 12.013a1 1 0 100-2 1 1 0 000 2zm0-3a.75.75 0 100-1.5.75.75 0 000 1.5z"
  }));
};

var icon = EuiIconVisGoal;
exports.icon = icon;