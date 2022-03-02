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
var EuiIconAppUsersRoles = function EuiIconAppUsersRoles(_ref) {
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
    d: "M19.307 3.21a2.91 2.91 0 10-.223 1.94 11.636 11.636 0 018.232 7.049l1.775-.698a13.576 13.576 0 00-9.784-8.291zm-2.822 1.638a.97.97 0 110-1.939.97.97 0 010 1.94zm-4.267.805l-.717-1.774a13.576 13.576 0 00-8.291 9.784 2.91 2.91 0 101.94.223 11.636 11.636 0 017.068-8.233zm-8.34 11.802a.97.97 0 110-1.94.97.97 0 010 1.94zm12.607 8.727a2.91 2.91 0 00-2.599 1.62 11.636 11.636 0 01-8.233-7.05l-1.774.717a13.576 13.576 0 009.813 8.291 2.91 2.91 0 102.793-3.578zm0 3.879a.97.97 0 110-1.94.97.97 0 010 1.94zM32 16.485a2.91 2.91 0 10-4.199 2.599 11.636 11.636 0 01-7.05 8.232l.718 1.775a13.576 13.576 0 008.291-9.813A2.91 2.91 0 0032 16.485zm-2.91.97a.97.97 0 110-1.94.97.97 0 010 1.94z"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillSecondary",
    d: "M19.19 16.35a3.879 3.879 0 10-5.42 0 4.848 4.848 0 00-2.134 4.014v1.939h9.697v-1.94a4.848 4.848 0 00-2.143-4.014zm-4.645-2.774a1.94 1.94 0 113.88 0 1.94 1.94 0 01-3.88 0zm-.97 6.788a2.91 2.91 0 115.819 0h-5.818z"
  }));
};

var icon = EuiIconAppUsersRoles;
exports.icon = icon;