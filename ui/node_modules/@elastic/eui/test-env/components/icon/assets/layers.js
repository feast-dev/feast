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
var EuiIconLayers = function EuiIconLayers(_ref) {
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
    d: "M7.276 1.053a.5.5 0 01.448 0l6 3a.5.5 0 010 .894l-6 3a.5.5 0 01-.448 0l-6-3a.5.5 0 010-.894l6-3zM2.618 4.5L7.5 6.941 12.382 4.5 7.5 2.059 2.618 4.5z"
  }), (0, _react2.jsx)("path", {
    d: "M1.053 7.276a.5.5 0 01.67-.223L7.5 9.94l5.776-2.888a.5.5 0 11.448.894l-6 3a.5.5 0 01-.448 0l-6-3a.5.5 0 01-.223-.67z"
  }), (0, _react2.jsx)("path", {
    d: "M1.724 10.053a.5.5 0 10-.448.894l6 3a.5.5 0 00.448 0l6-3a.5.5 0 10-.448-.894L7.5 12.94l-5.776-2.888z"
  }));
};

var icon = EuiIconLayers;
exports.icon = icon;