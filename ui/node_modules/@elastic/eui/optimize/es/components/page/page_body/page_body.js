import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

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