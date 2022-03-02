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
var EuiIconScale = function EuiIconScale(_ref) {
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
    d: "M12.5 12a.5.5 0 110 1 .5.5 0 010-1zm-2 0a.5.5 0 110 1 .5.5 0 010-1zm-2 0a.5.5 0 110 1 .5.5 0 010-1zm4-2a.5.5 0 110 1 .5.5 0 010-1zm-2 0a.5.5 0 110 1 .5.5 0 010-1zm2-1a.5.5 0 110-1 .5.5 0 010 1zm0-3a.5.5 0 110 1 .5.5 0 010-1zm-2 2a.5.5 0 110 1 .5.5 0 010-1zm-2 0a.5.5 0 110 1 .5.5 0 010-1zm0 2a.5.5 0 110 1 .5.5 0 010-1zm-2 2a.5.5 0 110 1 .5.5 0 010-1zm-2 0a.5.5 0 110 1 .5.5 0 010-1zm2-2a.5.5 0 110 1 .5.5 0 010-1zm6-6a.5.5 0 110 1 .5.5 0 010-1zm-2 2a.5.5 0 110 1 .5.5 0 010-1z"
  }));
};

var icon = EuiIconScale;
exports.icon = icon;