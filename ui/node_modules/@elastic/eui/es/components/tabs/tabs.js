function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { forwardRef } from 'react';
import classNames from 'classnames';
import { keysOf } from '../common';
import { jsx as ___EmotionJSX } from "@emotion/react";
var displayToClassNameMap = {
  condensed: 'euiTabs--condensed',
  default: null
};
export var DISPLAYS = keysOf(displayToClassNameMap);
var sizeToClassNameMap = {
  s: 'euiTabs--small',
  m: null,
  l: 'euiTabs--large',
  xl: 'euiTabs--xlarge'
};
export var SIZES = keysOf(sizeToClassNameMap);
export var EuiTabs = /*#__PURE__*/forwardRef(function (_ref, ref) {
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
      rest = _objectWithoutProperties(_ref, ["children", "className", "display", "bottomBorder", "expand", "size"]);

  /**
   * Temporary force of bottom border based on `display`
   */
  bottomBorder = display === 'condensed' ? false : bottomBorder;
  var classes = classNames('euiTabs', sizeToClassNameMap[size], displayToClassNameMap[display], {
    'euiTabs--expand': expand,
    'euiTabs--bottomBorder': bottomBorder
  }, className);
  return ___EmotionJSX("div", _extends({
    ref: ref,
    className: classes
  }, children && {
    role: 'tablist'
  }, rest), children);
});
EuiTabs.displayName = 'EuiTabs';