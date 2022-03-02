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
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  full: 'euiHorizontalRule--full',
  half: 'euiHorizontalRule--half',
  quarter: 'euiHorizontalRule--quarter'
};
export var SIZES = Object.keys(sizeToClassNameMap);
var marginToClassNameMap = {
  none: null,
  xs: 'euiHorizontalRule--marginXSmall',
  s: 'euiHorizontalRule--marginSmall',
  m: 'euiHorizontalRule--marginMedium',
  l: 'euiHorizontalRule--marginLarge',
  xl: 'euiHorizontalRule--marginXLarge',
  xxl: 'euiHorizontalRule--marginXXLarge'
};
export var MARGINS = Object.keys(marginToClassNameMap);
export var EuiHorizontalRule = function EuiHorizontalRule(_ref) {
  var className = _ref.className,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'full' : _ref$size,
      _ref$margin = _ref.margin,
      margin = _ref$margin === void 0 ? 'l' : _ref$margin,
      rest = _objectWithoutProperties(_ref, ["className", "size", "margin"]);

  var classes = classNames('euiHorizontalRule', sizeToClassNameMap[size], marginToClassNameMap[margin], className);
  return ___EmotionJSX("hr", _extends({
    className: classes
  }, rest));
};
EuiHorizontalRule.propTypes = {
  /**
     * Defines the width of the HR.
     */
  size: PropTypes.oneOf(["full", "half", "quarter"]),
  margin: PropTypes.oneOf(["none", "xs", "s", "m", "l", "xl", "xxl"]),
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};