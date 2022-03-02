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
var EuiIconReturnKey = function EuiIconReturnKey(_ref) {
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
    d: "M11.994 4c1.059 0 1.924.818 2 1.856l.006.15v1.988a2.005 2.005 0 01-1.856 2L12 10H3.484l1.91 1.82a.52.52 0 010 .77.616.616 0 01-.829 0l-2.05-1.95a1.551 1.551 0 010-2.31l2.05-1.95a.617.617 0 01.83 0 .52.52 0 010 .77L3.45 9H12c.514-.003.935-.39.993-.888L13 7.994V6.006c0-.516-.388-.941-.888-1L11.994 5H9.5a.5.5 0 01-.09-.992L9.5 4h2.494z"
  }));
};

var icon = EuiIconReturnKey;
exports.icon = icon;