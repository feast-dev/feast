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