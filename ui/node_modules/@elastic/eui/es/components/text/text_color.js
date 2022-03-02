function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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
import PropTypes from "prop-types";
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
EuiTextColor.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Any of our named colors or a `hex`, `rgb` or `rgba` value.
       */
  color: PropTypes.oneOfType([PropTypes.oneOf(["default", "subdued", "success", "accent", "danger", "warning", "ghost", "inherit"]).isRequired, PropTypes.any.isRequired]),

  /**
       * Determines the root element
       */
  component: PropTypes.oneOf(["div", "span"])
};