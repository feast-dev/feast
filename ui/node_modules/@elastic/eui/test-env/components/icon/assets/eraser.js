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
var EuiIconEraser = function EuiIconEraser(_ref) {
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
    d: "M2.648 9.937l7.29-7.288a2.21 2.21 0 013.124 0l2.29 2.288a2.21 2.21 0 010 3.126L10.413 13H12.5a.5.5 0 010 1H4.501a2.21 2.21 0 01-1.563-.647l.707-.707c.227.226.535.354.856.354h4.005a1.21 1.21 0 00.848-.354l1.292-1.293-4-4-3.29 3.291a1.21 1.21 0 000 1.712l.29.29-.708.707-.29-.29a2.21 2.21 0 010-3.126zM8 6h6.89a1.208 1.208 0 00-.246-.356L14 5H9L8 6zm2-2h3l-.645-.644a1.21 1.21 0 00-1.71 0L10 4zm4.89 3H7.708l1 1H14l.644-.644A1.22 1.22 0 0014.891 7zM9.708 9l1.646 1.646L13 9H9.707z"
  }), (0, _react2.jsx)("path", {
    d: "M14 11.5a.5.5 0 11-1 0 .5.5 0 011 0zm1.5-.5a.5.5 0 100-1 .5.5 0 000 1zm-1 2a.5.5 0 000 1h1a.5.5 0 000-1h-1z"
  }));
};

var icon = EuiIconEraser;
exports.icon = icon;