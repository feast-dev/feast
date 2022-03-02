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
var EuiIconMagnet = function EuiIconMagnet(_ref) {
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
    d: "M4.552 10.71a3.008 3.008 0 004.19.738l1.642-1.15 1.15 1.642-1.643 1.15a5.013 5.013 0 11-5.75-8.212l1.642-1.15 1.15 1.643-1.642 1.15a3.007 3.007 0 00-.739 4.189zm8.296-2.137l1.15 1.643-1.643 1.149-1.15-1.642 1.643-1.15zm-4.6-6.571l1.15 1.643-1.643 1.15-1.15-1.642 1.642-1.151zm1.97 1.068L9.07 1.428a1.003 1.003 0 00-1.397-.246L3.566 4.057A5.995 5.995 0 001.092 7.94a5.993 5.993 0 00.996 4.495 5.99 5.99 0 003.883 2.473 5.991 5.991 0 004.495-.996l4.107-2.875c.454-.318.563-.943.246-1.396l-1.15-1.643a1.002 1.002 0 00-1.396-.246l-4.107 2.875a2.002 2.002 0 01-1.498.332 2 2 0 01-1.627-2.323c.09-.505.371-.976.824-1.294l4.107-2.876c.454-.317.564-.942.246-1.396z"
  }));
};

var icon = EuiIconMagnet;
exports.icon = icon;