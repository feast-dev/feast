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
var EuiIconCrosshairs = function EuiIconCrosshairs(_ref) {
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
    d: "M5.822 1.874a.5.5 0 11.335.942 5.517 5.517 0 00-3.34 3.341.5.5 0 11-.943-.335 6.517 6.517 0 013.948-3.948zM1.864 10.15a.5.5 0 11.944-.33 5.517 5.517 0 003.365 3.37.5.5 0 01-.333.943 6.517 6.517 0 01-3.976-3.983zm8.302 3.981a.5.5 0 11-.333-.943 5.517 5.517 0 003.347-3.332.5.5 0 11.941.337 6.517 6.517 0 01-3.955 3.938zm3.968-8.285a.5.5 0 11-.943.331A5.517 5.517 0 009.85 2.82a.5.5 0 01.337-.942 6.517 6.517 0 013.946 3.968zM8.5 3.5a.5.5 0 01-1 0V.997a.5.5 0 011 0V3.5zm-4.997 4a.5.5 0 010 1H1a.5.5 0 010-1h2.503zM7.5 12.497a.5.5 0 011 0V15a.5.5 0 11-1 0v-2.503zM12.497 8.5a.5.5 0 010-1H15a.5.5 0 110 1h-2.503zM8 9a1 1 0 110-2 1 1 0 010 2z"
  }));
};

var icon = EuiIconCrosshairs;
exports.icon = icon;