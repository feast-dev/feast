"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiPageBody = exports.PADDING_SIZES = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var _react = _interopRequireDefault(require("react"));

var _classnames = _interopRequireDefault(require("classnames"));

var _common = require("../../common");

var _restrict_width = require("../_restrict_width");

var _panel = require("../../panel");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var paddingSizeToClassNameMap = {
  none: null,
  s: 'euiPageBody--paddingSmall',
  m: 'euiPageBody--paddingMedium',
  l: 'euiPageBody--paddingLarge'
};
var PADDING_SIZES = (0, _common.keysOf)(paddingSizeToClassNameMap);
exports.PADDING_SIZES = PADDING_SIZES;

var EuiPageBody = function EuiPageBody(_ref) {
  var children = _ref.children,
      _ref$restrictWidth = _ref.restrictWidth,
      restrictWidth = _ref$restrictWidth === void 0 ? false : _ref$restrictWidth,
      style = _ref.style,
      className = _ref.className,
      _ref$component = _ref.component,
      Component = _ref$component === void 0 ? 'div' : _ref$component,
      panelled = _ref.panelled,
      panelProps = _ref.panelProps,
      paddingSize = _ref.paddingSize,
      _ref$borderRadius = _ref.borderRadius,
      borderRadius = _ref$borderRadius === void 0 ? 'none' : _ref$borderRadius,
      rest = (0, _objectWithoutProperties2.default)(_ref, ["children", "restrictWidth", "style", "className", "component", "panelled", "panelProps", "paddingSize", "borderRadius"]);

  var _setPropsForRestricte = (0, _restrict_width.setPropsForRestrictedPageWidth)(restrictWidth, style),
      widthClassName = _setPropsForRestricte.widthClassName,
      newStyle = _setPropsForRestricte.newStyle;

  var nonBreakingDefaultPadding = panelled ? 'l' : 'none';
  paddingSize = paddingSize || nonBreakingDefaultPadding;
  var borderRadiusClass = borderRadius === 'none' ? 'euiPageBody--borderRadiusNone' : '';
  var classes = (0, _classnames.default)('euiPageBody', borderRadiusClass, // This may duplicate the padding styles from EuiPanel, but allows for some nested configurations in the CSS
  paddingSizeToClassNameMap[paddingSize], (0, _defineProperty2.default)({}, "euiPageBody--".concat(widthClassName), widthClassName), className);
  return panelled ? (0, _react2.jsx)(_panel.EuiPanel, (0, _extends2.default)({
    className: classes,
    style: newStyle || style,
    borderRadius: borderRadius,
    paddingSize: paddingSize
  }, panelProps, rest), children) : (0, _react2.jsx)(Component, (0, _extends2.default)({
    className: classes,
    style: newStyle || style
  }, rest), children);
};

exports.EuiPageBody = EuiPageBody;