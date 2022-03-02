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
var EuiIconLogoEtcd = function EuiIconLogoEtcd(_ref) {
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
    fill: "#419EDA"
  }, (0, _react2.jsx)("path", {
    d: "M14.65 14.164c0 1.189-.933 2.15-2.083 2.15-1.152 0-2.082-.961-2.082-2.15 0-1.185.93-2.15 2.082-2.15 1.15 0 2.083.965 2.083 2.15zm2.693 0c0 1.189.934 2.15 2.084 2.15s2.083-.961 2.083-2.15c0-1.185-.933-2.15-2.083-2.15-1.15 0-2.084.965-2.084 2.15z"
  }), (0, _react2.jsx)("path", {
    d: "M31.522 16.495a5.845 5.845 0 01-3.105-.618c.272-1.603.387-3.218.357-4.827a24.896 24.896 0 00-3.027-3.703 6.137 6.137 0 012.133-2.36l.391-.25-.304-.354c-1.57-1.825-3.444-3.235-5.568-4.193L21.975 0l-.107.462a6.248 6.248 0 01-1.51 2.829 23.77 23.77 0 00-4.362-1.855 23.79 23.79 0 00-4.355 1.851A6.246 6.246 0 0110.137.465l-.108-.462-.423.189c-2.097.943-4.022 2.392-5.566 4.19l-.306.355.39.25a6.125 6.125 0 012.129 2.347 25.001 25.001 0 00-3.022 3.689 26.068 26.068 0 00.347 4.86 5.832 5.832 0 01-3.09.612L.03 16.46l.043.473a16.723 16.723 0 002.14 6.765l.234.41.35-.308a5.914 5.914 0 012.837-1.372 25.255 25.255 0 002.429 4.083c1.475.533 3.013.93 4.601 1.167a6.359 6.359 0 01-.383 3.243l-.175.439.45.102c1.15.261 2.31.394 3.447.394l3.447-.394.45-.102-.175-.44a6.376 6.376 0 01-.383-3.244 23.905 23.905 0 004.585-1.165 25.286 25.286 0 002.43-4.086 5.94 5.94 0 012.851 1.373l.35.306.233-.408a16.661 16.661 0 002.14-6.764l.042-.472-.45.036zM20.93 22.278c-1.63.458-3.287.689-4.933.689-1.65 0-3.304-.231-4.936-.69a20.028 20.028 0 01-2.113-4.594 20.072 20.072 0 01-.89-5.04 19.076 19.076 0 013.576-3.516 18.867 18.867 0 014.363-2.448 18.944 18.944 0 014.355 2.443 19.19 19.19 0 013.592 3.538c-.09 1.73-.39 3.41-.899 5.019a19.965 19.965 0 01-2.115 4.599z"
  })));
};

var icon = EuiIconLogoEtcd;
exports.icon = icon;