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
var EuiIconPush = function EuiIconPush(_ref) {
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
    d: "M8.171 5.15L10.114 7H1.556C1.249 7 1 7.224 1 7.5s.249.5.556.5h8.526l-1.91 1.82a.52.52 0 000 .77c.227.213.6.213.828 0l2.05-1.95a1.552 1.552 0 000-2.31L9 4.38a.617.617 0 00-.829 0 .52.52 0 000 .77z"
  }), (0, _react2.jsx)("path", {
    d: "M6.804 12.792A.993.993 0 016 11.82V10H5v1.826c0 .945.673 1.76 1.608 1.945l6 1.19A1.992 1.992 0 0015 13.016V1.984A2 2 0 0012.608.04l-6 1.19C5.673 1.415 5 2.23 5 3.175V5h1V3.18c0-.472.336-.879.804-.972l6-1.189A1 1 0 0114 1.991v11.018a.995.995 0 01-1.196.972l-6-1.19z"
  }));
};

var icon = EuiIconPush;
exports.icon = icon;