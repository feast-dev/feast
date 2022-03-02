import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";
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
import chroma from 'chroma-js';
import { keysOf } from '../common';
import { euiPaletteColorBlindBehindText, getSecureRelForTarget, isColorDark } from '../../services';
import { EuiInnerText } from '../inner_text';
import { EuiIcon } from '../icon';
import { chromaValid, parseColor } from '../color_picker/utils';
import { validateHref } from '../../services/security/href_validator';
import { jsx as ___EmotionJSX } from "@emotion/react";
// TODO - replace with variables once https://github.com/elastic/eui/issues/2731 is closed
var colorInk = '#000';
var colorGhost = '#fff'; // The color blind palette has some stricter accessibility needs with regards to
// charts and contrast. We use the euiPaletteColorBlindBehindText variant here since our
// accessibility concerns pertain to foreground (text) and background contrast

var visColors = euiPaletteColorBlindBehindText();
var colorToHexMap = {
  // TODO - replace with variable once https://github.com/elastic/eui/issues/2731 is closed
  default: '#d3dae6',
  primary: visColors[1],
  success: visColors[0],
  accent: visColors[2],
  warning: visColors[5],
  danger: visColors[9]
};
export var COLORS = keysOf(colorToHexMap);
var iconSideToClassNameMap = {
  left: 'euiBadge--iconLeft',
  right: 'euiBadge--iconRight'
};
export var ICON_SIDES = keysOf(iconSideToClassNameMap);
export var EuiBadge = function EuiBadge(_ref) {
  var children = _ref.children,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'default' : _ref$color,
      iconType = _ref.iconType,
      _ref$iconSide = _ref.iconSide,
      iconSide = _ref$iconSide === void 0 ? 'left' : _ref$iconSide,
      className = _ref.className,
      _isDisabled = _ref.isDisabled,
      onClick = _ref.onClick,
      iconOnClick = _ref.iconOnClick,
      onClickAriaLabel = _ref.onClickAriaLabel,
      iconOnClickAriaLabel = _ref.iconOnClickAriaLabel,
      closeButtonProps = _ref.closeButtonProps,
      href = _ref.href,
      rel = _ref.rel,
      target = _ref.target,
      style = _ref.style,
      rest = _objectWithoutProperties(_ref, ["children", "color", "iconType", "iconSide", "className", "isDisabled", "onClick", "iconOnClick", "onClickAriaLabel", "iconOnClickAriaLabel", "closeButtonProps", "href", "rel", "target", "style"]);

  var isHrefValid = !href || validateHref(href);
  var isDisabled = _isDisabled || !isHrefValid;
  var optionalCustomStyles = style;
  var textColor = null; // TODO - replace with variable once https://github.com/elastic/eui/issues/2731 is closed

  var wcagContrastBase = 4.5; // WCAG AA contrast level

  var wcagContrast = null;
  var colorHex = null; // Check if a valid color name was provided

  try {
    if (COLORS.indexOf(color) > -1) {
      // Get the hex equivalent for the provided color name
      colorHex = colorToHexMap[color]; // Set dark or light text color based upon best contrast

      textColor = setTextColor(colorHex);
      optionalCustomStyles = _objectSpread({
        backgroundColor: colorHex,
        color: textColor
      }, optionalCustomStyles);
    } else if (color !== 'hollow') {
      // This is a custom color that is neither from the base palette nor hollow
      // Let's do our best to ensure that it provides sufficient contrast
      // Set dark or light text color based upon best contrast
      textColor = setTextColor(color); // Check the contrast

      wcagContrast = getColorContrast(textColor, color);

      if (wcagContrast < wcagContrastBase) {
        // It's low contrast, so lets show a warning in the console
        console.warn('Warning: ', color, ' badge has low contrast of ', wcagContrast.toFixed(2), '. Should be above ', wcagContrastBase, '.');
      }

      optionalCustomStyles = _objectSpread({
        backgroundColor: color,
        color: textColor
      }, optionalCustomStyles);
    }
  } catch (err) {
    handleInvalidColor(color);
  }

  var classes = classNames('euiBadge', {
    'euiBadge-isClickable': (onClick || href) && !iconOnClick,
    'euiBadge-isDisabled': isDisabled,
    'euiBadge--hollow': color === 'hollow'
  }, iconSideToClassNameMap[iconSide], className);
  var closeClassNames = classNames('euiBadge__icon', closeButtonProps && closeButtonProps.className);
  var Element = href && !isDisabled ? 'a' : 'button';
  var relObj = {};

  if (href && !isDisabled) {
    relObj.href = href;
    relObj.target = target;
    relObj.rel = getSecureRelForTarget({
      href: href,
      target: target,
      rel: rel
    });
  }

  if (onClick) {
    relObj.onClick = onClick;
  }

  var optionalIcon = null;

  if (iconType) {
    if (iconOnClick) {
      if (!iconOnClickAriaLabel) {
        console.warn('When passing the iconOnClick props to EuiBadge, you must also provide iconOnClickAriaLabel');
      }

      optionalIcon = ___EmotionJSX("button", {
        type: "button",
        className: "euiBadge__iconButton",
        "aria-label": iconOnClickAriaLabel,
        disabled: isDisabled,
        title: iconOnClickAriaLabel,
        onClick: iconOnClick
      }, ___EmotionJSX(EuiIcon, _extends({
        type: iconType,
        size: "s",
        color: "inherit" // forces the icon to inherit its parent color

      }, closeButtonProps, {
        className: closeClassNames
      })));
    } else {
      optionalIcon = ___EmotionJSX(EuiIcon, {
        type: iconType,
        size: children ? 's' : 'm',
        className: "euiBadge__icon",
        color: "inherit" // forces the icon to inherit its parent color

      });
    }
  }

  if (onClick && !onClickAriaLabel) {
    console.warn('When passing onClick to EuiBadge, you must also provide onClickAriaLabel');
  }

  var content = ___EmotionJSX("span", {
    className: "euiBadge__content"
  }, children && ___EmotionJSX("span", {
    className: "euiBadge__text"
  }, children), optionalIcon);

  if (iconOnClick) {
    return onClick || href ? ___EmotionJSX("span", {
      className: classes,
      style: optionalCustomStyles
    }, ___EmotionJSX("span", {
      className: "euiBadge__content"
    }, ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
      return ___EmotionJSX(Element, _extends({
        className: "euiBadge__childButton",
        disabled: isDisabled,
        "aria-label": onClickAriaLabel,
        ref: ref,
        title: innerText
      }, relObj, rest), children);
    }), optionalIcon)) : ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
      return ___EmotionJSX("span", _extends({
        className: classes,
        style: optionalCustomStyles,
        ref: ref,
        title: innerText
      }, rest), content);
    });
  } else if (onClick || href) {
    return ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
      return ___EmotionJSX(Element, _extends({
        disabled: isDisabled,
        "aria-label": onClickAriaLabel,
        className: classes,
        style: optionalCustomStyles,
        ref: ref,
        title: innerText
      }, relObj, rest), content);
    });
  } else {
    return ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
      return ___EmotionJSX("span", _extends({
        className: classes,
        style: optionalCustomStyles,
        ref: ref,
        title: innerText
      }, rest), content);
    });
  }
};

function getColorContrast(textColor, color) {
  var contrastValue = chroma.contrast(textColor, color);
  return contrastValue;
}

function setTextColor(bgColor) {
  var textColor = isColorDark.apply(void 0, _toConsumableArray(chroma(bgColor).rgb())) ? colorGhost : colorInk;
  return textColor;
}

function handleInvalidColor(color) {
  var isNamedColor = color && COLORS.includes(color) || color === 'hollow';
  var isValidColorString = color && chromaValid(parseColor(color) || '');

  if (!isNamedColor && !isValidColorString) {
    console.warn('EuiBadge expects a valid color. This can either be a three or six ' + "character hex value, rgb(a) value, hsv value, hollow, or one of the following: ".concat(COLORS, ". ") + "Instead got ".concat(color, "."));
  }
}