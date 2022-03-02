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
import { keysOf } from '../common';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  m: 'euiLoadingChart--medium',
  l: 'euiLoadingChart--large',
  xl: 'euiLoadingChart--xLarge'
};
export var SIZES = keysOf(sizeToClassNameMap);
export var EuiLoadingChart = function EuiLoadingChart(_ref) {
  var _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      _ref$mono = _ref.mono,
      mono = _ref$mono === void 0 ? false : _ref$mono,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["size", "mono", "className"]);

  var classes = classNames('euiLoadingChart', {
    'euiLoadingChart--mono': mono
  }, className, sizeToClassNameMap[size]);
  return ___EmotionJSX("span", _extends({
    className: classes
  }, rest), ___EmotionJSX("span", {
    className: "euiLoadingChart__bar"
  }), ___EmotionJSX("span", {
    className: "euiLoadingChart__bar"
  }), ___EmotionJSX("span", {
    className: "euiLoadingChart__bar"
  }), ___EmotionJSX("span", {
    className: "euiLoadingChart__bar"
  }));
};