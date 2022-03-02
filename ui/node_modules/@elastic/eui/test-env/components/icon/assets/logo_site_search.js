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
var EuiIconLogoSiteSearch = function EuiIconLogoSiteSearch(_ref) {
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
  }, title) : null, (0, _react2.jsx)("g", {
    fill: "none",
    fillRule: "evenodd"
  }, (0, _react2.jsx)("path", {
    fill: "#FA744E",
    d: "M27.05 10h-7.34l-11 22s15.696-4.96 21.855-16.076C32.037 13.266 30.088 10 27.05 10"
  }), (0, _react2.jsx)("path", {
    fill: "#00BFB3",
    d: "M21.355 0H7.533L.427 14.211C-.903 16.871 1.032 20 4.004 20h7.351l10-20z"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillNegative",
    d: "M2.533 10L.428 14.211C-.903 16.871 1.032 20 4.005 20h7.35l5-10H2.533z"
  })));
};

var icon = EuiIconLogoSiteSearch;
exports.icon = icon;