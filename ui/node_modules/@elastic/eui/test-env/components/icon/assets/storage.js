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
var EuiIconStorage = function EuiIconStorage(_ref) {
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
  }, title) : null, (0, _react2.jsx)("g", {
    fillRule: "evenodd",
    transform: "translate(0 2)"
  }, (0, _react2.jsx)("path", {
    fillRule: "nonzero",
    d: "M2 6a1 1 0 01-1-1V2a1 1 0 011-1h12a1 1 0 011 1v3a1 1 0 01-1 1H2zm13 2v3a1 1 0 01-1 1H2a1 1 0 01-1-1V8a1 1 0 011-1h12a1 1 0 011 1zm1-3V2a2 2 0 00-2-2H2a2 2 0 00-2 2v3c0 .601.271 1.133.69 1.5C.271 6.867 0 7.399 0 8v3a2 2 0 002 2h12a2 2 0 002-2V8c0-.601-.271-1.133-.689-1.5.418-.367.689-.899.689-1.5z"
  }), (0, _react2.jsx)("circle", {
    cx: 4.5,
    cy: 9.5,
    r: 1.5
  }), (0, _react2.jsx)("circle", {
    cx: 4.5,
    cy: 3.5,
    r: 1.5
  }), (0, _react2.jsx)("path", {
    d: "M12 8h1v3h-1zM10 8h1v3h-1zM12 2h1v3h-1zM10 2h1v3h-1z"
  })));
};

var icon = EuiIconStorage;
exports.icon = icon;