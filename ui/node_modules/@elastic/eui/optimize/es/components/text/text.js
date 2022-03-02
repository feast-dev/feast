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
import { EuiTextColor } from './text_color';
import { EuiTextAlign } from './text_align';
import { jsx as ___EmotionJSX } from "@emotion/react";
var textSizeToClassNameMap = {
  xs: 'euiText--extraSmall',
  s: 'euiText--small',
  m: 'euiText--medium',
  relative: 'euiText--relative'
};
export var TEXT_SIZES = keysOf(textSizeToClassNameMap);
export var EuiText = function EuiText(_ref) {
  var _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      color = _ref.color,
      _ref$grow = _ref.grow,
      grow = _ref$grow === void 0 ? true : _ref$grow,
      textAlign = _ref.textAlign,
      children = _ref.children,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["size", "color", "grow", "textAlign", "children", "className"]);

  var classes = classNames('euiText', textSizeToClassNameMap[size], className, {
    'euiText--constrainedWidth': !grow
  });
  var optionallyAlteredText;

  if (color) {
    optionallyAlteredText = ___EmotionJSX(EuiTextColor, {
      color: color,
      component: "div"
    }, children);
  }

  if (textAlign) {
    optionallyAlteredText = ___EmotionJSX(EuiTextAlign, {
      textAlign: textAlign
    }, optionallyAlteredText || children);
  }

  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), optionallyAlteredText || children);
};