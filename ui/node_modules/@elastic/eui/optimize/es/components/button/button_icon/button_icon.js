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
import { getSecureRelForTarget } from '../../../services';
import { keysOf } from '../../common';
import { EuiIcon } from '../../icon';
import { validateHref } from '../../../services/security/href_validator';
import { jsx as ___EmotionJSX } from "@emotion/react";
var displayToClassNameMap = {
  base: null,
  empty: 'euiButtonIcon--empty',
  fill: 'euiButtonIcon--fill'
};
export var DISPLAYS = keysOf(displayToClassNameMap);
var colorToClassNameMap = {
  accent: 'euiButtonIcon--accent',
  danger: 'euiButtonIcon--danger',
  ghost: 'euiButtonIcon--ghost',
  primary: 'euiButtonIcon--primary',
  success: 'euiButtonIcon--success',
  text: 'euiButtonIcon--text',
  warning: 'euiButtonIcon--warning'
};
export var COLORS = keysOf(colorToClassNameMap);
var sizeToClassNameMap = {
  xs: 'euiButtonIcon--xSmall',
  s: 'euiButtonIcon--small',
  m: 'euiButtonIcon--medium'
};
export var SIZES = keysOf(sizeToClassNameMap);
export var EuiButtonIcon = function EuiButtonIcon(_ref) {
  var className = _ref.className,
      iconType = _ref.iconType,
      _ref$iconSize = _ref.iconSize,
      iconSize = _ref$iconSize === void 0 ? 'm' : _ref$iconSize,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'primary' : _ref$color,
      _isDisabled = _ref.isDisabled,
      disabled = _ref.disabled,
      href = _ref.href,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'button' : _ref$type,
      _ref$display = _ref.display,
      display = _ref$display === void 0 ? 'empty' : _ref$display,
      target = _ref.target,
      rel = _ref.rel,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'xs' : _ref$size,
      buttonRef = _ref.buttonRef,
      isSelected = _ref.isSelected,
      rest = _objectWithoutProperties(_ref, ["className", "iconType", "iconSize", "color", "isDisabled", "disabled", "href", "type", "display", "target", "rel", "size", "buttonRef", "isSelected"]);

  var isHrefValid = !href || validateHref(href);
  var isDisabled = _isDisabled || disabled || !isHrefValid;
  var ariaHidden = rest['aria-hidden'];
  var isAriaHidden = ariaHidden === 'true' || ariaHidden === true;

  if (!rest['aria-label'] && !rest['aria-labelledby'] && !isAriaHidden) {
    console.warn("EuiButtonIcon requires aria-label or aria-labelledby to be specified because icon-only\n      buttons are screen-reader-inaccessible without them.");
  }

  var classes = classNames('euiButtonIcon', {
    'euiButtonIcon-isDisabled': isDisabled
  }, colorToClassNameMap[color], display && displayToClassNameMap[display], size && sizeToClassNameMap[size], className); // Add an icon to the button if one exists.

  var buttonIcon;

  if (iconType) {
    buttonIcon = ___EmotionJSX(EuiIcon, {
      className: "euiButtonIcon__icon",
      type: iconType,
      size: iconSize,
      "aria-hidden": "true",
      color: "inherit" // forces the icon to inherit its parent color

    });
  } // <a> elements don't respect the `disabled` attribute. So if we're disabled, we'll just pretend
  // this is a button and piggyback off its disabled styles.


  if (href && !isDisabled) {
    var secureRel = getSecureRelForTarget({
      href: href,
      target: target,
      rel: rel
    });
    return ___EmotionJSX("a", _extends({
      tabIndex: isAriaHidden ? -1 : undefined,
      className: classes,
      href: href,
      target: target,
      rel: secureRel,
      ref: buttonRef
    }, rest), buttonIcon);
  }

  var buttonType;
  return ___EmotionJSX("button", _extends({
    tabIndex: isAriaHidden ? -1 : undefined,
    disabled: isDisabled,
    className: classes,
    "aria-pressed": isSelected,
    type: type,
    ref: buttonRef
  }, rest), buttonIcon);
};