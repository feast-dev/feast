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
var EuiIconTraining = function EuiIconTraining(_ref) {
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
    d: "M10.386 9.836a2.5 2.5 0 113.611.667C15.212 11.173 16 12.46 16 14v1.5a.5.5 0 11-1 0V14c0-1.724-1.276-3-3-3-.91 0-1.298-.02-1.805-.122-1.25-.254-2.333-1-3.585-2.566a.5.5 0 11.78-.624c.9 1.124 1.653 1.74 2.434 2.043.155.052.345.083.562.105zm1.785.128c.083.01.167.021.251.034L12.5 10a1.5 1.5 0 10-.33-.036zM9.78 11.97a.5.5 0 01.5.5c0 .076-.047.226-.05.231-.179.38-.23.774-.23 1.302v1.5a.5.5 0 11-1 0v-1.5c0-.657.072-1.186.307-1.696a.5.5 0 01.473-.337zM5.958 5.772a.5.5 0 01-.78.625L3.11 3.812a.5.5 0 11.78-.624l2.068 2.584zM1 11h5.5a.5.5 0 110 1h-6a.5.5 0 01-.5-.5V.5A.5.5 0 01.5 0h12a.5.5 0 01.5.5v3a.5.5 0 11-1 0V1H1v10z"
  }));
};

var icon = EuiIconTraining;
exports.icon = icon;