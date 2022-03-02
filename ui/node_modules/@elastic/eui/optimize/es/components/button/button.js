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
import React, { forwardRef } from 'react';
import classNames from 'classnames';
import { keysOf } from '../common';
import { getSecureRelForTarget } from '../../services';
import { EuiButtonContent } from './button_content';
import { validateHref } from '../../services/security/href_validator';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var colorToClassNameMap = {
  primary: '--primary',
  accent: '--accent',
  success: '--success',
  warning: '--warning',
  danger: '--danger',
  ghost: '--ghost',
  text: '--text'
};
export var COLORS = keysOf(colorToClassNameMap);
export var sizeToClassNameMap = {
  s: '--small',
  m: null
};
export var SIZES = keysOf(sizeToClassNameMap);
/**
 * Extends EuiButtonContentProps which provides
 * `iconType`, `iconSide`, and `textProps`
 */

/**
 * *INTERNAL ONLY*
 * Component for displaying any element as a button
 * EuiButton is largely responsible for providing relevant props
 * and the logic for element-specific attributes
 */
var EuiButtonDisplay = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var _ref$element = _ref.element,
      element = _ref$element === void 0 ? 'button' : _ref$element,
      baseClassName = _ref.baseClassName,
      children = _ref.children,
      className = _ref.className,
      iconType = _ref.iconType,
      _ref$iconSide = _ref.iconSide,
      iconSide = _ref$iconSide === void 0 ? 'left' : _ref$iconSide,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'primary' : _ref$color,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      _ref$fill = _ref.fill,
      fill = _ref$fill === void 0 ? false : _ref$fill,
      isDisabled = _ref.isDisabled,
      isLoading = _ref.isLoading,
      isSelected = _ref.isSelected,
      contentProps = _ref.contentProps,
      textProps = _ref.textProps,
      fullWidth = _ref.fullWidth,
      minWidth = _ref.minWidth,
      style = _ref.style,
      rest = _objectWithoutProperties(_ref, ["element", "baseClassName", "children", "className", "iconType", "iconSide", "color", "size", "fill", "isDisabled", "isLoading", "isSelected", "contentProps", "textProps", "fullWidth", "minWidth", "style"]);

  var buttonIsDisabled = isLoading || isDisabled;
  var classes = classNames(baseClassName, color && colorToClassNameMap[color] ? "".concat(baseClassName).concat(colorToClassNameMap[color]) : "".concat(baseClassName).concat(colorToClassNameMap.primary), size && sizeToClassNameMap[size] ? "".concat(baseClassName).concat(sizeToClassNameMap[size]) : null, fill && "".concat(baseClassName, "--fill"), fullWidth && "".concat(baseClassName, "--fullWidth"), buttonIsDisabled && "".concat(baseClassName, "-isDisabled"), className);
  /**
   * Not changing the content or text class names to match baseClassName yet,
   * as it is a major breaking change.
   */

  var contentClassNames = classNames('euiButton__content', contentProps && contentProps.className);
  var textClassNames = classNames('euiButton__text', textProps && textProps.className);

  var innerNode = ___EmotionJSX(EuiButtonContent, _extends({
    isLoading: isLoading,
    iconType: iconType,
    iconSide: iconSide,
    textProps: _objectSpread(_objectSpread({}, textProps), {}, {
      className: textClassNames
    })
  }, contentProps, {
    // className has to come last to override contentProps.className
    className: contentClassNames
  }), children);

  var calculatedStyle = style;

  if (minWidth !== undefined || minWidth !== null) {
    calculatedStyle = _objectSpread(_objectSpread({}, calculatedStyle), {}, {
      minWidth: minWidth
    });
  }

  return /*#__PURE__*/React.createElement(element, _objectSpread({
    className: classes,
    style: calculatedStyle,
    disabled: element === 'button' && buttonIsDisabled,
    'aria-pressed': element === 'button' ? isSelected : undefined,
    ref: ref
  }, rest), innerNode);
});
EuiButtonDisplay.displayName = 'EuiButtonDisplay';
export { EuiButtonDisplay };
export var EuiButton = function EuiButton(_ref2) {
  var _isDisabled = _ref2.isDisabled,
      _disabled = _ref2.disabled,
      href = _ref2.href,
      target = _ref2.target,
      rel = _ref2.rel,
      _ref2$type = _ref2.type,
      type = _ref2$type === void 0 ? 'button' : _ref2$type,
      buttonRef = _ref2.buttonRef,
      rest = _objectWithoutProperties(_ref2, ["isDisabled", "disabled", "href", "target", "rel", "type", "buttonRef"]);

  var isHrefValid = !href || validateHref(href);
  var disabled = _disabled || !isHrefValid;
  var isDisabled = _isDisabled || !isHrefValid;
  var buttonIsDisabled = rest.isLoading || isDisabled || disabled;
  var element = href && !isDisabled ? 'a' : 'button';
  var elementProps = {}; // Props for all elements

  elementProps = _objectSpread(_objectSpread({}, elementProps), {}, {
    isDisabled: buttonIsDisabled
  }); // Element-specific attributes

  if (element === 'button') {
    elementProps = _objectSpread(_objectSpread({}, elementProps), {}, {
      disabled: buttonIsDisabled
    });
  }

  var relObj = {};

  if (href && !buttonIsDisabled) {
    relObj.href = href;
    relObj.rel = getSecureRelForTarget({
      href: href,
      target: target,
      rel: rel
    });
    relObj.target = target;
  } else {
    relObj.type = type;
  }

  return ___EmotionJSX(EuiButtonDisplay, _extends({
    element: element,
    baseClassName: "euiButton",
    ref: buttonRef
  }, elementProps, relObj, rest));
};