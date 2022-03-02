"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiTabs = exports.SIZES = exports.DISPLAYS = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var _react = _interopRequireWildcard(require("react"));

var _classnames = _interopRequireDefault(require("classnames"));

var _common = require("../common");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var displayToClassNameMap = {
  condensed: 'euiTabs--condensed',
  default: null
};
var DISPLAYS = (0, _common.keysOf)(displayToClassNameMap);
exports.DISPLAYS = DISPLAYS;
var sizeToClassNameMap = {
  s: 'euiTabs--small',
  m: null,
  l: 'euiTabs--large',
  xl: 'euiTabs--xlarge'
};
var SIZES = (0, _common.keysOf)(sizeToClassNameMap);
exports.SIZES = SIZES;
var EuiTabs = /*#__PURE__*/(0, _react.forwardRef)(function (_ref, ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$display = _ref.display,
      display = _ref$display === void 0 ? 'default' : _ref$display,
      _ref$bottomBorder = _ref.bottomBorder,
      bottomBorder = _ref$bottomBorder === void 0 ? true : _ref$bottomBorder,
      _ref$expand = _ref.expand,
      expand = _ref$expand === void 0 ? false : _ref$expand,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      rest = (0, _objectWithoutProperties2.default)(_ref, ["children", "className", "display", "bottomBorder", "expand", "size"]);

  /**
   * Temporary force of bottom border based on `display`
   */
  bottomBorder = display === 'condensed' ? false : bottomBorder;
  var classes = (0, _classnames.default)('euiTabs', sizeToClassNameMap[size], displayToClassNameMap[display], {
    'euiTabs--expand': expand,
    'euiTabs--bottomBorder': bottomBorder
  }, className);
  return (0, _react2.jsx)("div", (0, _extends2.default)({
    ref: ref,
    className: classes
  }, children && {
    role: 'tablist'
  }, rest), children);
});
exports.EuiTabs = EuiTabs;
EuiTabs.displayName = 'EuiTabs';