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
var EuiIconAccessibility = function EuiIconAccessibility(_ref) {
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
    d: "M8 0a8 8 0 110 16A8 8 0 018 0zm0 1a7 7 0 100 14A7 7 0 008 1zm3.974 4.342a.5.5 0 01-.233.596l-.083.036L9 6.86v2.559l.974 2.923a.5.5 0 01-.233.596l-.083.036a.5.5 0 01-.596-.233l-.036-.083-1-3L8 9.5l-.026.158-1 3a.5.5 0 01-.97-.228l.022-.088L7 9.416V6.86l-2.658-.886a.5.5 0 01.228-.97l.088.022L7.583 6h.833l2.926-.974a.5.5 0 01.632.316zM8 3a1 1 0 110 2 1 1 0 010-2z"
  }));
};

var icon = EuiIconAccessibility;
exports.icon = icon;