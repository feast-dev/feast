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
import { keysOf } from '../common';
import { setPropsForRestrictedPageWidth } from './_restrict_width';
import { jsx as ___EmotionJSX } from "@emotion/react";
var paddingSizeToClassNameMap = {
  none: null,
  s: 'euiPage--paddingSmall',
  m: 'euiPage--paddingMedium',
  l: 'euiPage--paddingLarge'
};
var directionToClassNameMap = {
  row: null,
  column: 'euiPage--column'
};
export var SIZES = keysOf(paddingSizeToClassNameMap);
export var DIRECTIONS = keysOf(directionToClassNameMap);
export var EuiPage = function EuiPage(_ref) {
  var children = _ref.children,
      _ref$restrictWidth = _ref.restrictWidth,
      restrictWidth = _ref$restrictWidth === void 0 ? false : _ref$restrictWidth,
      style = _ref.style,
      className = _ref.className,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'm' : _ref$paddingSize,
      _ref$grow = _ref.grow,
      grow = _ref$grow === void 0 ? true : _ref$grow,
      _ref$direction = _ref.direction,
      direction = _ref$direction === void 0 ? 'row' : _ref$direction,
      rest = _objectWithoutProperties(_ref, ["children", "restrictWidth", "style", "className", "paddingSize", "grow", "direction"]);

  var _setPropsForRestricte = setPropsForRestrictedPageWidth(restrictWidth, style),
      widthClassName = _setPropsForRestricte.widthClassName,
      newStyle = _setPropsForRestricte.newStyle;

  var classes = classNames('euiPage', paddingSizeToClassNameMap[paddingSize], directionToClassNameMap[direction], _defineProperty({
    'euiPage--grow': grow
  }, "euiPage--".concat(widthClassName), widthClassName), className);
  return ___EmotionJSX("div", _extends({
    className: classes,
    style: newStyle || style
  }, rest), children);
};