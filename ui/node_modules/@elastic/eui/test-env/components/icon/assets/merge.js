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
var EuiIconMerge = function EuiIconMerge(_ref) {
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
    d: "M7.352 6H2.5a.5.5 0 010-1h4.852L5.12 2.721c-.18-.183-.155-.46.055-.616a.551.551 0 01.705.048l3 3.062c.16.164.16.405 0 .57l-3 3.062A.532.532 0 015.5 9a.54.54 0 01-.325-.106c-.21-.157-.235-.433-.055-.616L7.352 6zm1.296 4H13.5a.5.5 0 010 1H8.648l2.232 2.278c.18.183.155.46-.055.617A.54.54 0 0110.5 14a.532.532 0 01-.38-.153l-3-3.063a.397.397 0 010-.568l3-3.063a.551.551 0 01.705-.047c.21.156.235.433.055.616L8.648 10z"
  }));
};

var icon = EuiIconMerge;
exports.icon = icon;