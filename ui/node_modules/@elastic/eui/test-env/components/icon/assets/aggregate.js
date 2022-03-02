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
var EuiIconAggregate = function EuiIconAggregate(_ref) {
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
    fillRule: "evenodd",
    d: "M2.5 2a.5.5 0 100 1 .5.5 0 000-1zm0-1a1.5 1.5 0 011.415 1h1.908a1.5 1.5 0 011.393.943L8.839 7H12.5a.5.5 0 010 1H8.839l-1.623 4.057A1.5 1.5 0 015.823 13H3.915a1.5 1.5 0 110-1h1.908a.5.5 0 00.464-.314L7.761 8H3.915a1.5 1.5 0 110-1H7.76L6.287 3.314A.5.5 0 005.823 3H3.915A1.5 1.5 0 112.5 1zm0 11a.5.5 0 110 1 .5.5 0 010-1zM3 7.5a.5.5 0 10-1 0 .5.5 0 001 0zm9.354-3.354a.5.5 0 00-.708.708L13.793 7a.707.707 0 010 1l-2.147 2.146a.5.5 0 00.708.708L14.5 8.707a1.707 1.707 0 000-2.414l-2.146-2.147z"
  }));
};

var icon = EuiIconAggregate;
exports.icon = icon;