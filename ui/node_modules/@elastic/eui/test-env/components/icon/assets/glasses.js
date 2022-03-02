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
var EuiIconGlasses = function EuiIconGlasses(_ref) {
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
    d: "M9.035 9A3.5 3.5 0 0115 7.05V4.5c0-1.072-.648-1.72-2.098-2.01a.5.5 0 01.196-.98C14.981 1.886 16 2.905 16 4.5v4.25c0 .072-.015.14-.043.202A3.5 3.5 0 119.035 10h-2.07A3.5 3.5 0 11.043 8.952.498.498 0 010 8.75V4.5c0-1.595 1.019-2.614 2.902-2.99a.5.5 0 01.196.98C1.648 2.78 1 3.428 1 4.5v2.55A3.5 3.5 0 016.965 9h2.07zM3.5 12a2.5 2.5 0 100-5 2.5 2.5 0 000 5zm9 0a2.5 2.5 0 100-5 2.5 2.5 0 000 5z"
  }));
};

var icon = EuiIconGlasses;
exports.icon = icon;