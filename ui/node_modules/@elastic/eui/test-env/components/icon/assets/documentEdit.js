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
var EuiIconDocumentEdit = function EuiIconDocumentEdit(_ref) {
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
    d: "M8.505 8.995l6.453-6.44-1.5-1.5-6.453 6.44 1.5 1.5zM12.968.19c.258-.238.657-.26.91 0l1.928 1.929a.642.642 0 010 .909l-6.78 6.784A.641.641 0 018.57 10H6.643A.643.643 0 016 9.357V7.43c0-.17.067-.335.188-.455L12.968.19zM4.5 13a.5.5 0 110-1h7a.5.5 0 110 1h-7zm4-12a.5.5 0 010 1H2v13h12V7.5a.5.5 0 111 0V15a1 1 0 01-1 1H2a1 1 0 01-1-1V2a1 1 0 011-1h6.5z"
  }));
};

var icon = EuiIconDocumentEdit;
exports.icon = icon;