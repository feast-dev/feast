function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import classNames from 'classnames';
import { keysOf } from '../../common';
import { setPropsForRestrictedPageWidth } from '../_restrict_width';
import { EuiPanel } from '../../panel';
import { jsx as ___EmotionJSX } from "@emotion/react";
var paddingSizeToClassNameMap = {
  none: null,
  s: 'euiPageBody--paddingSmall',
  m: 'euiPageBody--paddingMedium',
  l: 'euiPageBody--paddingLarge'
};
export var PADDING_SIZES = keysOf(paddingSizeToClassNameMap);
export var EuiPageBody = function EuiPageBody(_ref) {
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
      rest = _objectWithoutProperties(_ref, ["children", "restrictWidth", "style", "className", "component", "panelled", "panelProps", "paddingSize", "borderRadius"]);

  var _setPropsForRestricte = setPropsForRestrictedPageWidth(restrictWidth, style),
      widthClassName = _setPropsForRestricte.widthClassName,
      newStyle = _setPropsForRestricte.newStyle;

  var nonBreakingDefaultPadding = panelled ? 'l' : 'none';
  paddingSize = paddingSize || nonBreakingDefaultPadding;
  var borderRadiusClass = borderRadius === 'none' ? 'euiPageBody--borderRadiusNone' : '';
  var classes = classNames('euiPageBody', borderRadiusClass, // This may duplicate the padding styles from EuiPanel, but allows for some nested configurations in the CSS
  paddingSizeToClassNameMap[paddingSize], _defineProperty({}, "euiPageBody--".concat(widthClassName), widthClassName), className);
  return panelled ? ___EmotionJSX(EuiPanel, _extends({
    className: classes,
    style: newStyle || style,
    borderRadius: borderRadius,
    paddingSize: paddingSize
  }, panelProps, rest), children) : ___EmotionJSX(Component, _extends({
    className: classes,
    style: newStyle || style
  }, rest), children);
};