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
var EuiIconIndexRuntime = function EuiIconIndexRuntime(_ref) {
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
    d: "M12 2H2v11h6v1H1V1h12v6.839l-1-.707V2z"
  }), (0, _react2.jsx)("path", {
    d: "M8 11v-1H5v1h3zm0-3.055c0-.342.082-.664.23-.945H5v1h3v-.055zM5 5h5.999V4H5v1zM3 4v1h1V4H3zm0 3v1h1V7H3zm0 3v1h1v-1H3zm11.607.2l-4.32-3.055C9.727 6.75 9 7.204 9 7.945v6.108c0 .74.726 1.196 1.287.8l4.32-3.055c.524-.37.524-1.228 0-1.598z"
  }));
};

var icon = EuiIconIndexRuntime;
exports.icon = icon;