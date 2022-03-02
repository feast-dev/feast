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
var EuiIconLogstashFilter = function EuiIconLogstashFilter(_ref) {
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
    d: "M7.914 1c-3.6 0-5.897 1.111-5.897 1.876 0 .766 2.297 1.877 5.897 1.877s5.897-1.111 5.897-1.877C13.811 2.111 11.514 1 7.914 1m3.98 7.988c-1.187.331-2.601.502-3.98.502-1.383 0-2.801-.172-3.991-.505l2.863 3.242v2.515c.136.08.515.214 1.128.214.61 0 .989-.133 1.126-.213v-2.515l2.854-3.24zm2.394-1.206l-4.248 4.823v2.246h.004c0 .763-1.069 1.105-2.13 1.105-1.06 0-2.13-.342-2.13-1.105h.002v-2.245L1.321 7.55l.01-.008A1.53 1.53 0 011 6.607V2.78h.02C1.14.973 4.627 0 7.913 0c3.286 0 6.774.973 6.894 2.78h.02v3.827c0 .343-.12.657-.335.941l-.003.004c-.06.079-.128.155-.202.23zm-.578-.856a.56.56 0 00.118-.319V4.411c-1.288.879-3.649 1.342-5.914 1.342C5.65 5.753 3.288 5.29 2 4.411v2.196C2 7.375 4.304 8.49 7.914 8.49c2.879 0 4.927-.709 5.639-1.385l.157-.18z"
  }));
};

var icon = EuiIconLogstashFilter;
exports.icon = icon;