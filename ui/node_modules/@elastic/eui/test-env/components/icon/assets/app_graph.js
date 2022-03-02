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
var EuiIconAppGraph = function EuiIconAppGraph(_ref) {
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
  }, title) : null, (0, _react2.jsx)("path", {
    className: "euiIcon__fillSecondary",
    d: "M24 20a4 4 0 110-8 4 4 0 010 8zm0-6a2 2 0 100 4 2 2 0 000-4zm-8.2-5.62A4 4 0 1118 1.06a4 4 0 01-2.2 7.32zm0-6a2 2 0 10.01 0h-.01zm.01 29.24a4 4 0 11-.083-8 4 4 0 01.083 8zm0-6a2 2 0 10.39 0 2 2 0 00-.4 0h.01z"
  }), (0, _react2.jsx)("path", {
    d: "M18 17v-2h-6.14a4 4 0 00-.86-1.64l2.31-3.44-1.68-1.12-2.31 3.44A4 4 0 008 12a4 4 0 100 8 4 4 0 001.32-.24l2.31 3.44 1.66-1.12L11 18.64a4 4 0 00.86-1.64H18zM6 16a2 2 0 114 0 2 2 0 01-4 0z"
  }));
};

var icon = EuiIconAppGraph;
exports.icon = icon;