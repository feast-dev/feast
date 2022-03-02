import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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
var colorsToClassNameMap = {
  default: 'euiTextColor--default',
  subdued: 'euiTextColor--subdued',
  success: 'euiTextColor--success',
  accent: 'euiTextColor--accent',
  danger: 'euiTextColor--danger',
  warning: 'euiTextColor--warning',
  ghost: 'euiTextColor--ghost',
  inherit: 'euiTextColor--inherit'
};
export var COLORS = keysOf(colorsToClassNameMap);
export var EuiTextColor = function EuiTextColor(_ref) {
  var children = _ref.children,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'default' : _ref$color,
      className = _ref.className,
      _ref$component = _ref.component,
      component = _ref$component === void 0 ? 'span' : _ref$component,
      style = _ref.style,
      rest = _objectWithoutProperties(_ref, ["children", "color", "className", "component", "style"]);

  var isNamedColor = COLORS.includes(color);
  var classes = classNames('euiTextColor', {
    'euiTextColor--custom': !isNamedColor
  }, isNamedColor && colorsToClassNameMap[color], className);
  var Component = component; // We're checking if is a custom color.
  // If it is a custom color we set the `color` of the `.euiTextColor` div to that custom color.
  // This way the children text elements can `inherit` that color and border and backgrounds can get that `currentColor`.

  var euiTextStyle = !isNamedColor ? _objectSpread({
    color: color
  }, style) : _objectSpread({}, style);
  return ___EmotionJSX(Component, _extends({
    className: classes,
    style: euiTextStyle
  }, rest), children);
};