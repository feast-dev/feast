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
var EuiIconVisLine = function EuiIconVisLine(_ref) {
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
    d: "M12.654 3.48c.248.225.552.389.888.467L11.24 9.43a1.99 1.99 0 00-.915-.404l2.33-5.547zM9.146 9.19a2.008 2.008 0 00-.769.64l-1.572-2c.311-.136.581-.35.785-.618l1.556 1.978zM5.581 7.956l-2.134 4.268a.5.5 0 01-.894-.448l2.134-4.268c.25.22.557.376.894.448zM1 15h13.5a.5.5 0 110 1H.5a.5.5 0 01-.5-.5v-14a.5.5 0 011 0V15zm5-8a1 1 0 110-2 1 1 0 010 2zm4 5a1 1 0 110-2 1 1 0 010 2zm4-9a1 1 0 110-2 1 1 0 010 2z"
  }));
};

var icon = EuiIconVisLine;
exports.icon = icon;