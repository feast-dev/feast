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
var EuiIconVisGauge = function EuiIconVisGauge(_ref) {
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
    d: "M12.877 5.847l-1.02 1.02a.5.5 0 01-.708-.707l1.1-1.099c-.05-.053-.1-.106-.152-.157A6.471 6.471 0 008 3.019V4.5a.5.5 0 01-1 0V3.019a6.47 6.47 0 00-4.261 2.055l1.07 1.071a.5.5 0 01-.706.707l-.99-.99A6.46 6.46 0 001.018 10H2.5a.5.5 0 110 1H1.174c.083.353.196.697.337 1.03a.5.5 0 11-.922.39A7.487 7.487 0 010 9.5a7.483 7.483 0 012.197-5.304A7.487 7.487 0 017.5 2a7.487 7.487 0 015.304 2.197A7.483 7.483 0 0115 9.5a7.487 7.487 0 01-.59 2.92.5.5 0 01-.92-.39c.14-.333.253-.677.336-1.03H12.5a.5.5 0 110-1h1.481a6.483 6.483 0 00-1.104-4.153zm-6.041 5.317a.993.993 0 01-.01-1.404c.384-.385 2.882-2.002 3.149-1.735.267.267-1.35 2.765-1.735 3.15a.993.993 0 01-1.404-.01z"
  }));
};

var icon = EuiIconVisGauge;
exports.icon = icon;