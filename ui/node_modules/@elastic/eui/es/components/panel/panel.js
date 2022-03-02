function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
EuiPanel.propTypes = {
  element: PropTypes.oneOfType([PropTypes.oneOf(["button"]), PropTypes.oneOf(["div"])]),

  /**
     * Adds a medium shadow to the panel;
     * Only works when `color="plain"`
     */

  /**
     * Adds a medium shadow to the panel;
     * Only works when `color="plain"`
     */
  hasShadow: PropTypes.bool,

  /**
     * Adds a slight 1px border on all edges.
     * Only works when `color="plain | transparent"`
     * Default is `undefined` and will default to that theme's panel style
     */

  /**
     * Adds a slight 1px border on all edges.
     * Only works when `color="plain | transparent"`
     * Default is `undefined` and will default to that theme's panel style
     */
  hasBorder: PropTypes.bool,

  /**
     * Padding for all four sides
     */

  /**
     * Padding for all four sides
     */
  paddingSize: PropTypes.any,

  /**
     * Corner border radius
     */

  /**
     * Corner border radius
     */
  borderRadius: PropTypes.any,

  /**
     * When true the panel will grow in height to match `EuiFlexItem`
     */

  /**
     * When true the panel will grow in height to match `EuiFlexItem`
     */
  grow: PropTypes.bool,
  panelRef: PropTypes.any,

  /**
     * Background color of the panel;
     * Usually a lightened form of the brand colors
     */

  /**
     * Background color of the panel;
     * Usually a lightened form of the brand colors
     */
  color: PropTypes.any,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};