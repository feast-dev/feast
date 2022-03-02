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
var EuiIconUnlink = function EuiIconUnlink(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = (0, _objectWithoutProperties2.default)(_ref, ["title", "titleId"]);
  return (0, _react2.jsx)("svg", (0, _extends2.default)({
    xmlns: "http://www.w3.org/2000/svg",
    width: 16,
    height: 16,
    fill: "none",
    viewBox: "0 0 16 16",
    "aria-labelledby": titleId
  }, props), title ? (0, _react2.jsx)("title", {
    id: titleId
  }, title) : null, (0, _react2.jsx)("path", {
    d: "M7.565 10.83a.5.5 0 11.819.573l-1.877 2.68c-.963 1.376-3.02 1.55-4.588.45C.35 13.436-.191 11.445.773 10.07L3.64 5.973c.963-1.375 3.019-1.548 4.587-.45a.5.5 0 11-.573.82C6.509 5.54 5.08 5.66 4.46 6.546l-2.868 4.095c-.621.887-.245 2.27.9 3.073 1.146.802 2.575.682 3.196-.205l1.877-2.68z"
  }), (0, _react2.jsx)("path", {
    d: "M7.892 3.43a.5.5 0 11-.574-.819L10 .734c1.376-.963 3.367-.422 4.465 1.146 1.098 1.569.926 3.625-.45 4.588L9.918 9.336c-1.375.963-3.366.422-4.464-1.146a.5.5 0 11.819-.574c.802 1.146 2.185 1.522 3.072.9L13.44 5.65c.886-.621 1.006-2.05.204-3.195-.802-1.146-2.186-1.522-3.072-.9L7.892 3.43zM6 .5v3a.5.5 0 01-1 0v-3a.5.5 0 011 0zM1.058 2.23l2.458 1.72a.5.5 0 01-.574.82L.484 3.05a.5.5 0 11.574-.82zm12 7.093l2.457 1.72a.5.5 0 11-.573.82l-2.457-1.72a.5.5 0 11.573-.82zm-2.099 1.181v3a.5.5 0 11-1 0v-3a.5.5 0 111 0z"
  }));
};

var icon = EuiIconUnlink;
exports.icon = icon;