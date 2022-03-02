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
var EuiIconEditorLink = function EuiIconEditorLink(_ref) {
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
    d: "M7.556 5.051a.45.45 0 00.637.637l1.503-1.504c.432-.431 1.278-.382 1.89.23.612.612.662 1.458.23 1.89L9.519 8.6c-.432.432-1.278.383-1.89-.23a.45.45 0 10-.636.637c.914.914 2.33 1.063 3.162.23l2.297-2.297c.833-.833.684-2.248-.23-3.162-.914-.915-2.33-1.063-3.162-.23L7.556 5.051zm.888 5.261a.45.45 0 00-.637 0l-1.503 1.504c-.432.431-1.278.382-1.89-.23-.612-.612-.661-1.458-.23-1.89L6.481 7.4c.432-.432 1.278-.383 1.89.23a.45.45 0 00.636-.637c-.914-.914-2.33-1.063-3.162-.23L3.548 9.06c-.833.833-.685 2.248.23 3.162.914.915 2.33 1.063 3.162.23l1.504-1.503a.45.45 0 000-.637zM7.877 5.76a.39.39 0 00.274-.114l1.503-1.504-1.503 1.504a.39.39 0 01-.274.114zm.912 3.183c-.4.003-.843-.172-1.202-.53.359.358.802.533 1.202.53zM12.18 3.82c-.502-.503-1.155-.766-1.773-.76.618-.006 1.27.257 1.773.76.898.898 1.034 2.275.23 3.078l-2.297 2.297 2.297-2.297c.804-.803.668-2.18-.23-3.078zm-4.062 6.42a.39.39 0 01.284.667L6.898 12.41l1.504-1.503a.39.39 0 00-.284-.667zm-.926-3.965c.618-.006 1.27.257 1.773.76-.502-.503-1.155-.766-1.773-.76zM5.414 12.15a1.762 1.762 0 01-1.042-.522c-.626-.627-.692-1.511-.23-1.974L6.44 7.358 4.142 9.654c-.462.463-.396 1.348.23 1.974.311.311.687.484 1.042.522z"
  }));
};

var icon = EuiIconEditorLink;
exports.icon = icon;