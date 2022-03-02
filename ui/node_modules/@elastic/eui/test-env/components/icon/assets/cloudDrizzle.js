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
var EuiIconCloudDrizzle = function EuiIconCloudDrizzle(_ref) {
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
    d: "M6.348 3.761A3.995 3.995 0 018 7a.5.5 0 01-1 0 3 3 0 10-4.878 2.34.5.5 0 01-.627.779 4 4 0 013.973-6.84 5.502 5.502 0 0110.096 4.37.5.5 0 11-.92-.39 4.5 4.5 0 10-8.296-3.497zm-1.61 4.935a.5.5 0 11.775.633l-1.466 1.792a.5.5 0 11-.774-.633l1.466-1.792zm-3.12 3.647a.5.5 0 01.774.634l-1.505 1.84a.5.5 0 01-.774-.634l1.505-1.84zm7.62-3.647a.5.5 0 01.775.633l-1.466 1.792a.5.5 0 11-.774-.633l1.466-1.792zm-3.12 3.647a.5.5 0 01.774.634l-1.505 1.84a.5.5 0 01-.774-.634l1.505-1.84zm7.62-3.647a.5.5 0 11.775.633l-1.466 1.792a.5.5 0 11-.774-.633l1.466-1.792zm-3.12 3.647a.5.5 0 01.774.634l-1.505 1.84a.5.5 0 01-.774-.634l1.505-1.84z"
  }));
};

var icon = EuiIconCloudDrizzle;
exports.icon = icon;