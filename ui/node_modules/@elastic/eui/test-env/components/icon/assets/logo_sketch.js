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
var EuiIconLogoSketch = function EuiIconLogoSketch(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = (0, _objectWithoutProperties2.default)(_ref, ["title", "titleId"]);
  return (0, _react2.jsx)("svg", (0, _extends2.default)({
    xmlns: "http://www.w3.org/2000/svg",
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    "aria-labelledby": titleId
  }, props), title ? (0, _react2.jsx)("title", {
    id: titleId
  }, title) : null, (0, _react2.jsx)("g", {
    fill: "none"
  }, (0, _react2.jsx)("path", {
    fill: "#FFAE00",
    d: "M16 32L0 11.454l6.931-10.38L16 0l9.069 1.074L32 11.454z"
  }), (0, _react2.jsx)("path", {
    fill: "#EC6C00",
    d: "M16 32L0 11.454h32z"
  }), (0, _react2.jsx)("path", {
    fill: "#FFAE00",
    d: "M16 32L6.477 11.454h19.045z"
  }), (0, _react2.jsx)("path", {
    fill: "#FFEFB4",
    d: "M16 0L6.477 11.454h19.045z"
  }), (0, _react2.jsx)("path", {
    fill: "#FFAE00",
    d: "M6.932 1.074L3.369 6.3.001 11.454h6.542zM25.069 1.074L28.632 6.3 32 11.454h-6.542z"
  }), (0, _react2.jsx)("path", {
    fill: "#FED305",
    d: "M6.931 1.074l-.453 10.38L16 0zM25.069 1.074l.453 10.38L16 0z"
  })));
};

var icon = EuiIconLogoSketch;
exports.icon = icon;