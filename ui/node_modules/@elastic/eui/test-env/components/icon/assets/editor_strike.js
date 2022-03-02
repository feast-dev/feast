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
var EuiIconEditorStrike = function EuiIconEditorStrike(_ref) {
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
    d: "M10.023 10h1.274c.006.08.01.164.01.25a2.557 2.557 0 01-.883 1.949c-.284.25-.627.446-1.03.588A4.087 4.087 0 018.028 13a4.616 4.616 0 01-3.382-1.426c-.193-.259-.193-.5 0-.724.193-.223.438-.266.735-.13.343.363.748.655 1.213.876.466.22.949.33 1.449.33.637 0 1.132-.144 1.485-.433.353-.29.53-.67.53-1.14a1.72 1.72 0 00-.034-.353zM5.586 7a2.49 2.49 0 01-.294-.507 2.316 2.316 0 01-.177-.934c0-.363.076-.701.228-1.015.152-.314.363-.586.633-.816.27-.23.588-.41.955-.537A3.683 3.683 0 018.145 3c.578 0 1.112.11 1.603.33.49.221.907.508 1.25.861.16.282.16.512 0 .692-.16.18-.38.214-.662.102a3.438 3.438 0 00-.978-.669 2.914 2.914 0 00-1.213-.242c-.54 0-.973.125-1.302.375-.328.25-.492.595-.492 1.036 0 .236.046.434.14.596.092.162.217.304.374.426.157.123.329.23.515.324.119.06.24.116.362.169H5.586zM2.5 8h11a.5.5 0 110 1h-11a.5.5 0 010-1z"
  }));
};

var icon = EuiIconEditorStrike;
exports.icon = icon;