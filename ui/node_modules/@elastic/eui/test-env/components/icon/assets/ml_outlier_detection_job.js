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
var EuiIconMlOutlierDetectionJob = function EuiIconMlOutlierDetectionJob(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = (0, _objectWithoutProperties2.default)(_ref, ["title", "titleId"]);
  return (0, _react2.jsx)("svg", (0, _extends2.default)({
    xmlns: "http://www.w3.org/2000/svg",
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    "aria-labelledby": titleId
  }, props), title ? (0, _react2.jsx)("title", {
    id: titleId
  }, title) : null, (0, _react2.jsx)("path", {
    d: "M2 12v6h6v2H0v-8h2zm18 0v8h-8v-2h6v-6h2zM8 0v2H2v6H0V0h8zm12 0v8h-2V2h-6V0h8z"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillSecondary",
    d: "M16 24a4 4 0 110 8 4 4 0 010-8zm12 0a4 4 0 110 8 4 4 0 010-8zm-12 1.75a2.25 2.25 0 100 4.5 2.25 2.25 0 000-4.5zm12 0a2.25 2.25 0 100 4.5 2.25 2.25 0 000-4.5zM28 12a4 4 0 110 8 4 4 0 010-8zm0 1.75a2.25 2.25 0 100 4.5 2.25 2.25 0 000-4.5zM10 6a4 4 0 110 8 4 4 0 010-8zm0 1.75a2.25 2.25 0 100 4.5 2.25 2.25 0 000-4.5z"
  }));
};

var icon = EuiIconMlOutlierDetectionJob;
exports.icon = icon;