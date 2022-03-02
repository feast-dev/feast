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
  xs: 'euiSpacer--xs',
  s: 'euiSpacer--s',
  m: 'euiSpacer--m',
  l: 'euiSpacer--l',
  xl: 'euiSpacer--xl',
  xxl: 'euiSpacer--xxl'
};
export var SIZES = Object.keys(sizeToClassNameMap);
export var EuiSpacer = function EuiSpacer(_ref) {
  var className = _ref.className,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'l' : _ref$size,
      rest = _objectWithoutProperties(_ref, ["className", "size"]);

  var classes = classNames('euiSpacer', sizeToClassNameMap[size], className);
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest));
};