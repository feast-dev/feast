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