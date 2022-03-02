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
import classnames from 'classnames';
import { keysOf } from '../../common';
import { EuiColorPaletteDisplayFixed } from './color_palette_display_fixed';
import { EuiColorPaletteDisplayGradient } from './color_palette_display_gradient';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  xs: 'euiColorPaletteDisplay--sizeExtraSmall',
  s: 'euiColorPaletteDisplay--sizeSmall',
  m: 'euiColorPaletteDisplay--sizeMedium'
};
export var SIZES = keysOf(sizeToClassNameMap);
export var EuiColorPaletteDisplay = function EuiColorPaletteDisplay(_ref) {
  var _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'fixed' : _ref$type,
      palette = _ref.palette,
      className = _ref.className,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 's' : _ref$size,
      rest = _objectWithoutProperties(_ref, ["type", "palette", "className", "size"]);

  var classes = classnames('euiColorPaletteDisplay', className, sizeToClassNameMap[size]);
  return ___EmotionJSX(React.Fragment, null, type === 'fixed' ? ___EmotionJSX(EuiColorPaletteDisplayFixed, _extends({
    className: classes,
    palette: palette
  }, rest)) : ___EmotionJSX(EuiColorPaletteDisplayGradient, _extends({
    className: classes,
    palette: palette
  }, rest)));
};
EuiColorPaletteDisplay.propTypes = {
  /**
     * Height of the palette display
     */
  size: PropTypes.oneOf(["xs", "s", "m"]),

  /**
     *   Specify the type of palette.
     *  `gradient`: each color fades into the next.
     */

  /**
     *  `fixed`: individual color blocks.
     */
  type: PropTypes.oneOfType([PropTypes.oneOf(["fixed"]), PropTypes.oneOf(["gradient"]).isRequired]),
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Array of color `strings` or an array of #ColorStop. The stops must be numbers in an ordered range.
     */

  /**
     * Array of color `strings` or an array of #ColorStop. The stops must be numbers in an ordered range.
     */
  palette: PropTypes.oneOfType([PropTypes.arrayOf(PropTypes.string.isRequired).isRequired, PropTypes.arrayOf(PropTypes.shape({
    stop: PropTypes.number.isRequired,
    color: PropTypes.string.isRequired
  }).isRequired).isRequired]).isRequired
};