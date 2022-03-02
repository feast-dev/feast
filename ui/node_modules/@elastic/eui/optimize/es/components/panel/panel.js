import _extends from "@babel/runtime/helpers/extends";
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
import { keysOf } from '../common';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var panelPaddingValues = {
  none: 0,
  s: 8,
  m: 16,
  l: 24
};
var paddingSizeToClassNameMap = {
  none: null,
  s: 'euiPanel--paddingSmall',
  m: 'euiPanel--paddingMedium',
  l: 'euiPanel--paddingLarge'
};
export var SIZES = keysOf(paddingSizeToClassNameMap);
var borderRadiusToClassNameMap = {
  none: 'euiPanel--borderRadiusNone',
  m: 'euiPanel--borderRadiusMedium'
};
export var BORDER_RADII = keysOf(borderRadiusToClassNameMap);
export var COLORS = ['transparent', 'plain', 'subdued', 'accent', 'primary', 'success', 'warning', 'danger'];
export var EuiPanel = function EuiPanel(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'm' : _ref$paddingSize,
      _ref$borderRadius = _ref.borderRadius,
      borderRadius = _ref$borderRadius === void 0 ? 'm' : _ref$borderRadius,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'plain' : _ref$color,
      _ref$hasShadow = _ref.hasShadow,
      hasShadow = _ref$hasShadow === void 0 ? true : _ref$hasShadow,
      hasBorder = _ref.hasBorder,
      _ref$grow = _ref.grow,
      grow = _ref$grow === void 0 ? true : _ref$grow,
      panelRef = _ref.panelRef,
      element = _ref.element,
      rest = _objectWithoutProperties(_ref, ["children", "className", "paddingSize", "borderRadius", "color", "hasShadow", "hasBorder", "grow", "panelRef", "element"]);

  // Shadows are only allowed when there's a white background (plain)
  var canHaveShadow = color === 'plain';
  var canHaveBorder = color === 'plain' || color === 'transparent';
  var classes = classNames('euiPanel', paddingSizeToClassNameMap[paddingSize], borderRadiusToClassNameMap[borderRadius], "euiPanel--".concat(color), {
    // The `no` classes turn off the option for default theme
    // While the `has` classes turn it on for Amsterdam
    'euiPanel--hasShadow': canHaveShadow && hasShadow === true,
    'euiPanel--noShadow': !canHaveShadow || hasShadow === false,
    'euiPanel--hasBorder': canHaveBorder && hasBorder === true,
    'euiPanel--noBorder': !canHaveBorder || hasBorder === false,
    'euiPanel--flexGrowZero': !grow,
    'euiPanel--isClickable': rest.onClick
  }, className);

  if (rest.onClick && element !== 'div') {
    return ___EmotionJSX("button", _extends({
      ref: panelRef,
      className: classes
    }, rest), children);
  }

  return ___EmotionJSX("div", _extends({
    ref: panelRef,
    className: classes
  }, rest), children);
};