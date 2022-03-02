function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
import { keysOf } from '../../common';
import { setPropsForRestrictedPageWidth } from '../_restrict_width';
import { jsx as ___EmotionJSX } from "@emotion/react";
var paddingSizeToClassNameMap = {
  none: null,
  s: 'euiPage--paddingSmall',
  m: 'euiPage--paddingMedium',
  l: 'euiPage--paddingLarge'
};
export var PADDING_SIZES = keysOf(paddingSizeToClassNameMap);
export var EuiPageContentBody = function EuiPageContentBody(_ref) {
  var children = _ref.children,
      _ref$restrictWidth = _ref.restrictWidth,
      restrictWidth = _ref$restrictWidth === void 0 ? false : _ref$restrictWidth,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'none' : _ref$paddingSize,
      style = _ref.style,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["children", "restrictWidth", "paddingSize", "style", "className"]);

  var _setPropsForRestricte = setPropsForRestrictedPageWidth(restrictWidth, style),
      widthClassName = _setPropsForRestricte.widthClassName,
      newStyle = _setPropsForRestricte.newStyle;

  var classes = classNames('euiPageContentBody', paddingSizeToClassNameMap[paddingSize], _defineProperty({}, "euiPage--".concat(widthClassName), widthClassName), className);
  return ___EmotionJSX("div", _extends({
    className: classes,
    style: newStyle || style
  }, rest), children);
};
EuiPageContentBody.propTypes = {
  /**
     * Adjust the padding.
     * When using this setting it's best to be consistent throughout all similar usages
     */
  paddingSize: PropTypes.any,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Sets the max-width of the page,
     * set to `true` to use the default size of `1000px (1200 for Amsterdam)`,
     * set to `false` to not restrict the width,
     * set to a number for a custom width in px,
     * set to a string for a custom width in custom measurement.
     */
  restrictWidth: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.number.isRequired, PropTypes.string.isRequired])
};