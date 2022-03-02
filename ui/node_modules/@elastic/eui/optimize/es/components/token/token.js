import _extends from "@babel/runtime/helpers/extends";
import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import defaults from 'lodash/defaults';
import classNames from 'classnames';
import { keysOf } from '../common';
import { isColorDark, hexToRgb } from '../../services';
import { EuiIcon } from '../icon';
import { TOKEN_MAP } from './token_map';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassMap = {
  xs: 'euiToken--xsmall',
  s: 'euiToken--small',
  m: 'euiToken--medium',
  l: 'euiToken--large'
};
export var SIZES = keysOf(sizeToClassMap);
var shapeToClassMap = {
  circle: 'euiToken--circle',
  square: 'euiToken--square',
  rectangle: 'euiToken--rectangle'
};
export var SHAPES = keysOf(shapeToClassMap);
var fillToClassMap = {
  none: null,
  light: 'euiToken--light',
  dark: 'euiToken--dark'
};
export var FILLS = keysOf(fillToClassMap);
var colorToClassMap = {
  euiColorVis0: 'euiToken--euiColorVis0',
  euiColorVis1: 'euiToken--euiColorVis1',
  euiColorVis2: 'euiToken--euiColorVis2',
  euiColorVis3: 'euiToken--euiColorVis3',
  euiColorVis4: 'euiToken--euiColorVis4',
  euiColorVis5: 'euiToken--euiColorVis5',
  euiColorVis6: 'euiToken--euiColorVis6',
  euiColorVis7: 'euiToken--euiColorVis7',
  euiColorVis8: 'euiToken--euiColorVis8',
  euiColorVis9: 'euiToken--euiColorVis9',
  gray: 'euiToken--gray'
};
export var COLORS = keysOf(colorToClassMap);
export var EuiToken = function EuiToken(_ref) {
  var iconType = _ref.iconType,
      color = _ref.color,
      fill = _ref.fill,
      shape = _ref.shape,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 's' : _ref$size,
      _ref$style = _ref.style,
      style = _ref$style === void 0 ? {} : _ref$style,
      className = _ref.className,
      title = _ref.title,
      ariaLabel = _ref['aria-label'],
      ariaLabelledby = _ref['aria-labelledby'],
      ariaDescribedby = _ref['aria-describedby'],
      rest = _objectWithoutProperties(_ref, ["iconType", "color", "fill", "shape", "size", "style", "className", "title", "aria-label", "aria-labelledby", "aria-describedby"]);

  // Set the icon size to the same as the passed size
  // unless they passed `xs` which IconSize doesn't support
  var finalSize = size === 'xs' ? 's' : size; // When displaying at the small size, the token specific icons
  // should actually be displayed at medium size

  if (typeof iconType === 'string' && iconType.indexOf('token') === 0 && size === 's') {
    finalSize = 'm';
  }

  var currentDisplay = {
    color: color,
    fill: fill,
    shape: shape
  };
  var finalDisplay; // If the iconType passed is one of the prefab token types,
  // grab its properties

  if (typeof iconType === 'string' && iconType in TOKEN_MAP) {
    var tokenDisplay = TOKEN_MAP[iconType];
    finalDisplay = defaults(currentDisplay, tokenDisplay);
  } else {
    finalDisplay = currentDisplay;
  }

  var finalColor = finalDisplay.color || 'gray';
  var finalShape = finalDisplay.shape || 'circle';
  var finalFill = finalDisplay.fill || 'light'; // Color can be a named space via euiColorVis

  var colorClass;

  if (finalColor in colorToClassMap) {
    colorClass = colorToClassMap[finalColor];
  } // Or it can be a string which adds inline styles for the
  else {
      // text color if fill='none' or
      if (finalFill === 'none') {
        style.color = finalColor;
      } // full background color if fill='dark' and overrides fill='light' with dark
      else {
          finalFill = 'dark';
          style.backgroundColor = finalColor;
          style.color = isColorDark.apply(void 0, _toConsumableArray(hexToRgb(finalColor))) ? '#FFFFFF' : '#000000';
        }
    }

  var classes = classNames('euiToken', colorClass, shapeToClassMap[finalShape], fillToClassMap[finalFill], sizeToClassMap[size], className);
  return ___EmotionJSX("span", _extends({
    className: classes,
    style: style
  }, rest), ___EmotionJSX(EuiIcon, {
    type: iconType,
    size: finalSize,
    title: title,
    "aria-label": ariaLabel,
    "aria-labelledby": ariaLabelledby,
    "aria-describedby": ariaDescribedby
  }));
};