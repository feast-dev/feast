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
var EuiIconVisVisualBuilder = function EuiIconVisVisualBuilder(_ref) {
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
    d: "M9.837 7c.11.93.165 1.886.165 2.869V13.5a.5.5 0 11-1 0V9.869A23.3 23.3 0 008.83 7H7.29c-.195 1.04-.292 1.985-.292 2.835V13.5a.5.5 0 11-1 0V9.835c0-.864.092-1.809.276-2.835H2.5a.5.5 0 01-.495-.57c.285-2.023 1.626-3.358 3.931-3.96 1.967-.514 4.22-.606 6.756-.278A1.5 1.5 0 0114 3.679V5.5A1.5 1.5 0 0112.5 7H9.837zm-.569-1H12.5a.5.5 0 00.5-.5V3.68a.5.5 0 00-.436-.497c-2.416-.311-4.54-.225-6.375.254C4.494 3.88 3.491 4.724 3.117 6H9.268zM2 10v3.5a.5.5 0 11-1 0v-4a.5.5 0 01.5-.5h3a.5.5 0 01.5.5v4a.5.5 0 11-1 0V10H2zm10 3.5a.5.5 0 11-1 0v-2a.5.5 0 01.5-.5h3a.5.5 0 01.5.5v2a.5.5 0 11-1 0V12h-2v1.5zM1.016 16.026a.5.5 0 010-1H15a.5.5 0 110 1H1.016z"
  }));
};

var icon = EuiIconVisVisualBuilder;
exports.icon = icon;