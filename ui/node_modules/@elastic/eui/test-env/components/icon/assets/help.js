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
var EuiIconHelp = function EuiIconHelp(_ref) {
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
    d: "M13.6 12.186l-1.357-1.358c-.025-.025-.058-.034-.084-.056.53-.794.84-1.746.84-2.773a4.977 4.977 0 00-.84-2.772c.026-.02.059-.03.084-.056L13.6 3.813a6.96 6.96 0 010 8.373zM8 15A6.956 6.956 0 013.814 13.6l1.358-1.358c.025-.025.034-.057.055-.084C6.02 12.688 6.974 13 8 13a4.978 4.978 0 002.773-.84c.02.026.03.058.056.083l1.357 1.358A6.956 6.956 0 018 15zm-5.601-2.813a6.963 6.963 0 010-8.373l1.359 1.358c.024.025.057.035.084.056A4.97 4.97 0 003 8c0 1.027.31 1.98.842 2.773-.027.022-.06.031-.084.056l-1.36 1.358zm5.6-.187A4 4 0 118 4a4 4 0 010 8zM8 1c1.573 0 3.019.525 4.187 1.4l-1.357 1.358c-.025.025-.035.057-.056.084A4.979 4.979 0 008 3a4.979 4.979 0 00-2.773.842c-.021-.027-.03-.059-.055-.084L3.814 2.4A6.957 6.957 0 018 1zm0-1a8.001 8.001 0 10.003 16.002A8.001 8.001 0 008 0z"
  }));
};

var icon = EuiIconHelp;
exports.icon = icon;