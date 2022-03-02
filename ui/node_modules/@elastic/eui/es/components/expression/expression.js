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
import { EuiIcon } from '../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
var colorToClassNameMap = {
  subdued: 'euiExpression--subdued',
  primary: 'euiExpression--primary',
  success: 'euiExpression--success',
  accent: 'euiExpression--accent',
  warning: 'euiExpression--warning',
  danger: 'euiExpression--danger'
};
var textWrapToClassNameMap = {
  'break-word': null,
  truncate: 'euiExpression--truncate'
};
export var COLORS = keysOf(colorToClassNameMap);
var displayToClassNameMap = {
  inline: null,
  columns: 'euiExpression--columns'
};
export var EuiExpression = function EuiExpression(_ref) {
  var className = _ref.className,
      description = _ref.description,
      descriptionProps = _ref.descriptionProps,
      value = _ref.value,
      valueProps = _ref.valueProps,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'success' : _ref$color,
      _ref$uppercase = _ref.uppercase,
      uppercase = _ref$uppercase === void 0 ? true : _ref$uppercase,
      _ref$isActive = _ref.isActive,
      isActive = _ref$isActive === void 0 ? false : _ref$isActive,
      _ref$display = _ref.display,
      display = _ref$display === void 0 ? 'inline' : _ref$display,
      _ref$descriptionWidth = _ref.descriptionWidth,
      descriptionWidth = _ref$descriptionWidth === void 0 ? '20%' : _ref$descriptionWidth,
      onClick = _ref.onClick,
      _ref$isInvalid = _ref.isInvalid,
      isInvalid = _ref$isInvalid === void 0 ? false : _ref$isInvalid,
      _ref$textWrap = _ref.textWrap,
      textWrap = _ref$textWrap === void 0 ? 'break-word' : _ref$textWrap,
      rest = _objectWithoutProperties(_ref, ["className", "description", "descriptionProps", "value", "valueProps", "color", "uppercase", "isActive", "display", "descriptionWidth", "onClick", "isInvalid", "textWrap"]);

  var calculatedColor = isInvalid ? 'danger' : color;
  var classes = classNames('euiExpression', className, {
    'euiExpression-isActive': isActive,
    'euiExpression-isClickable': onClick,
    'euiExpression-isUppercase': uppercase
  }, displayToClassNameMap[display], colorToClassNameMap[calculatedColor], textWrapToClassNameMap[textWrap]);
  var Component = onClick ? 'button' : 'span';
  var descriptionStyle = descriptionProps && descriptionProps.style;
  var customWidth = display === 'columns' && descriptionWidth ? _objectSpread({
    flexBasis: descriptionWidth
  }, descriptionStyle) : undefined;
  var invalidIcon = isInvalid ? ___EmotionJSX(EuiIcon, {
    className: "euiExpression__icon",
    type: "alert",
    color: calculatedColor
  }) : undefined;
  return ___EmotionJSX(Component, _extends({
    className: classes,
    onClick: onClick
  }, rest), ___EmotionJSX("span", _extends({
    className: "euiExpression__description",
    style: customWidth
  }, descriptionProps), description), ' ', value && ___EmotionJSX("span", _extends({
    className: "euiExpression__value"
  }, valueProps), value), invalidIcon);
};
EuiExpression.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * First part of the expression
     */

  /**
     * First part of the expression
     */
  description: PropTypes.node.isRequired,
  descriptionProps: PropTypes.any,

  /**
     * Second part of the expression
     */

  /**
     * Second part of the expression
     */
  value: PropTypes.node,
  valueProps: PropTypes.any,

  /**
     * Color of the `description`
     */

  /**
     * Color of the `description`
     */
  color: PropTypes.oneOf(["subdued", "primary", "success", "accent", "warning", "danger"]),

  /**
     * Should the `description` auto-uppercase?
     */

  /**
     * Should the `description` auto-uppercase?
     */
  uppercase: PropTypes.bool,

  /**
     * Adds an solid border at the bottom
     */

  /**
     * Adds an solid border at the bottom
     */
  isActive: PropTypes.bool,

  /**
     * Turns the component into a button and adds an editable style border at the bottom
     */

  /**
     * Turns the component into a button and adds an editable style border at the bottom
     */
  onClick: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.func, PropTypes.func.isRequired]), PropTypes.func]),

  /**
     * Sets the display style for the expression. Defaults to `inline`
     */

  /**
     * Sets the display style for the expression. Defaults to `inline`
     */
  display: PropTypes.oneOf(["inline", "columns"]),

  /**
     * Forces color to display as `danger` and shows an `alert` icon
     */

  /**
     * Forces color to display as `danger` and shows an `alert` icon
     */
  isInvalid: PropTypes.bool,

  /**
     * Sets a custom width for the description when using the columns layout.
     * Set to a number for a custom width in `px`.
     * Set to a string for a custom width in custom measurement.
     * Defaults to `20%`
     */

  /**
     * Sets a custom width for the description when using the columns layout.
     * Set to a number for a custom width in `px`.
     * Set to a string for a custom width in custom measurement.
     * Defaults to `20%`
     */
  descriptionWidth: PropTypes.oneOfType([PropTypes.number.isRequired, PropTypes.string.isRequired]),

  /**
     * Sets how to handle the wrapping of long text.
     */

  /**
     * Sets how to handle the wrapping of long text.
     */
  textWrap: PropTypes.oneOf(["break-word", "truncate"])
};