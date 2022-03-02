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
var EuiIconVisMapRegion = function EuiIconVisMapRegion(_ref) {
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
    d: "M6.5 2.309v8.69a.499.499 0 01-.032.176L9.5 12.691V3.809l-3-1.5zm-1-.04L2 3.825v8.906l3.527-1.568a.5.5 0 01-.027-.164V2.27zm.274-1.216a.498.498 0 01.471.01l3.768 1.884 4.284-1.904A.5.5 0 0115 1.5v10a.5.5 0 01-.297.457l-4.5 2a.5.5 0 01-.427-.01l-3.789-1.894-4.283 1.904a.5.5 0 01-.703-.457v-10a.5.5 0 01.297-.457l4.476-1.99zM10.5 3.825v8.906l3.5-1.556V2.27l-3.5 1.556z"
  }));
};

var icon = EuiIconVisMapRegion;
exports.icon = icon;