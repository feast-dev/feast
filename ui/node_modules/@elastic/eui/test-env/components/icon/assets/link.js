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
var EuiIconLink = function EuiIconLink(_ref) {
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
    d: "M7.66 3.803a.5.5 0 11-.706-.707L9.268.78c1.187-1.187 3.242-1 4.596.354s1.54 3.409.354 4.596l-3.536 3.536c-1.187 1.187-3.242 1-4.596-.354a.5.5 0 11.707-.707c.99.99 2.417 1.119 3.182.354l3.536-3.536c.765-.765.635-2.193-.354-3.182-.99-.99-2.417-1.119-3.182-.354L7.661 3.803zm-.32 7.392a.5.5 0 11.707.707l-2.315 2.314c-1.187 1.188-3.242 1-4.596-.353-1.354-1.354-1.54-3.41-.353-4.596L4.318 5.73c1.187-1.187 3.242-1 4.596.354a.5.5 0 01-.707.707c-.989-.99-2.416-1.12-3.182-.354L1.49 9.974c-.766.765-.636 2.193.353 3.182.99.989 2.417 1.119 3.182.353l2.315-2.314z"
  }));
};

var icon = EuiIconLink;
exports.icon = icon;