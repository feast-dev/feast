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
var EuiIconCurrency = function EuiIconCurrency(_ref) {
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
    d: "M12.95 1.636l1.414 1.414-2.192 2.193C12.695 6.033 13 6.98 13 8c0 1.02-.305 1.967-.828 2.757l2.192 2.193-1.414 1.414-2.193-2.192A4.977 4.977 0 018 13a4.977 4.977 0 01-2.757-.828L3.05 14.364 1.636 12.95l2.192-2.193A4.977 4.977 0 013 8c0-1.02.305-1.967.828-2.757L1.636 3.05 3.05 1.636l2.193 2.192A4.977 4.977 0 018 3c1.02 0 1.967.305 2.757.828l2.193-2.192zM8 5a2.99 2.99 0 00-1.168.236l-.126.057-.218.116-.132.081-.073.05a3.013 3.013 0 00-.241.187l-.113.103-.147.15c-.05.054-.097.11-.142.168l-.1.135-.05.073-.06.097c-.05.082-.096.166-.137.253l-.057.126A2.99 2.99 0 005 8c0 .414.084.809.236 1.168l.057.126.116.218.081.132c.059.089.121.175.189.257l.15.17.151.147c.056.051.114.1.174.147l.142.105c.054.037.109.072.165.106l-.124-.079.092.06.094.055c.436.247.94.388 1.477.388a2.99 2.99 0 001.168-.236l.125-.056.213-.113.151-.094.05-.034a3.011 3.011 0 00.323-.258l-.15.129.09-.075.168-.159.08-.084c.051-.056.1-.114.147-.174l.105-.142.106-.165c.047-.08.091-.161.131-.245l.057-.126A2.99 2.99 0 0011 8a2.99 2.99 0 00-.236-1.168l-.056-.125-.112-.211-.096-.155-.033-.049a3.011 3.011 0 00-.258-.322l.129.15-.075-.09-.159-.168-.084-.08a3.015 3.015 0 00-.174-.147l-.183-.132-.124-.079a2.993 2.993 0 00-.245-.131l-.126-.057A2.99 2.99 0 008 5z"
  }));
};

var icon = EuiIconCurrency;
exports.icon = icon;