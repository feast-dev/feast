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
import { keysOf } from '../../common';
import { getSecureRelForTarget } from '../../../services';
import { EuiButtonContent } from '../button_content';
import { validateHref } from '../../../services/security/href_validator';
import { jsx as ___EmotionJSX } from "@emotion/react";
var colorToClassNameMap = {
  primary: 'euiButtonEmpty--primary',
  danger: 'euiButtonEmpty--danger',
  text: 'euiButtonEmpty--text',
  ghost: 'euiButtonEmpty--ghost',
  success: 'euiButtonEmpty--success',
  warning: 'euiButtonEmpty--warning'
};
export var COLORS = keysOf(colorToClassNameMap);
var sizeToClassNameMap = {
  xs: 'euiButtonEmpty--xSmall',
  s: 'euiButtonEmpty--small',
  m: null
};
export var SIZES = keysOf(sizeToClassNameMap);
var flushTypeToClassNameMap = {
  left: 'euiButtonEmpty--flushLeft',
  right: 'euiButtonEmpty--flushRight',
  both: 'euiButtonEmpty--flushBoth'
};
export var FLUSH_TYPES = keysOf(flushTypeToClassNameMap);
/**
 * Extends EuiButtonContentProps which provides
 * `iconType`, `iconSide`, and `textProps`
 */

export var EuiButtonEmpty = function EuiButtonEmpty(_ref) {
  var children = _ref.children,
      className = _ref.className,
      iconType = _ref.iconType,
      _ref$iconSide = _ref.iconSide,
      iconSide = _ref$iconSide === void 0 ? 'left' : _ref$iconSide,
      _ref$iconSize = _ref.iconSize,
      iconSize = _ref$iconSize === void 0 ? 'm' : _ref$iconSize,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'primary' : _ref$color,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      flush = _ref.flush,
      _isDisabled = _ref.isDisabled,
      _disabled = _ref.disabled,
      isLoading = _ref.isLoading,
      href = _ref.href,
      target = _ref.target,
      rel = _ref.rel,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'button' : _ref$type,
      buttonRef = _ref.buttonRef,
      contentProps = _ref.contentProps,
      textProps = _ref.textProps,
      isSelected = _ref.isSelected,
      rest = _objectWithoutProperties(_ref, ["children", "className", "iconType", "iconSide", "iconSize", "color", "size", "flush", "isDisabled", "disabled", "isLoading", "href", "target", "rel", "type", "buttonRef", "contentProps", "textProps", "isSelected"]);

  var isHrefValid = !href || validateHref(href);
  var disabled = _disabled || !isHrefValid;
  var isDisabled = _isDisabled || !isHrefValid; // If in the loading state, force disabled to true

  var buttonIsDisabled = isLoading || isDisabled || disabled;
  var classes = classNames('euiButtonEmpty', colorToClassNameMap[color], size ? sizeToClassNameMap[size] : null, flush ? flushTypeToClassNameMap[flush] : null, {
    'euiButtonEmpty-isDisabled': buttonIsDisabled
  }, className);
  var contentClassNames = classNames('euiButtonEmpty__content', contentProps && contentProps.className);
  var textClassNames = classNames('euiButtonEmpty__text', textProps && textProps.className);

  var innerNode = ___EmotionJSX(EuiButtonContent, _extends({
    isLoading: isLoading,
    iconType: iconType,
    iconSide: iconSide,
    iconSize: size === 'xs' ? 's' : iconSize,
    textProps: _objectSpread(_objectSpread({}, textProps), {}, {
      className: textClassNames
    })
  }, contentProps, {
    // className has to come last to override contentProps.className
    className: contentClassNames
  }), children); // <a> elements don't respect the `disabled` attribute. So if we're disabled, we'll just pretend
  // this is a button and piggyback off its disabled styles.


  if (href && !buttonIsDisabled) {
    var secureRel = getSecureRelForTarget({
      href: href,
      target: target,
      rel: rel
    });
    return ___EmotionJSX("a", _extends({
      className: classes,
      href: href,
      target: target,
      rel: secureRel,
      ref: buttonRef
    }, rest), innerNode);
  }

  return ___EmotionJSX("button", _extends({
    disabled: buttonIsDisabled,
    className: classes,
    type: type,
    ref: buttonRef,
    "aria-pressed": isSelected
  }, rest), innerNode);
};