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
var EuiIconLogoEnterpriseSearch = function EuiIconLogoEnterpriseSearch(_ref) {
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
    fill: "#00BFB3",
    fillRule: "evenodd",
    d: "M16 0c-2.918 0-5.645.794-8 2.158 4.777 2.768 8 7.923 8 13.842 0 5.919-3.223 11.074-8 13.842A15.907 15.907 0 0016 32c8.837 0 16-7.163 16-16S24.837 0 16 0z",
    clipRule: "evenodd"
  }), (0, _react2.jsx)("path", {
    fill: "#FEC514",
    fillRule: "evenodd",
    d: "M8 24h2.222A12.996 12.996 0 0013 16c0-2.935-1.012-5.744-2.778-8H8a8 8 0 000 16z",
    clipRule: "evenodd"
  }), (0, _react2.jsx)("path", {
    className: "euiIcon__fillNegative",
    fillRule: "evenodd",
    d: "M16 8h-2.152A15.877 15.877 0 0116 16c0 2.918-.786 5.647-2.152 8H16a8 8 0 000-16z",
    clipRule: "evenodd"
  }));
};

var icon = EuiIconLogoEnterpriseSearch;
exports.icon = icon;