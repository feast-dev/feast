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
import { keysOf } from '../../common';
import { jsx as ___EmotionJSX } from "@emotion/react";
var colorToClassMap = {
  accent: null,
  subdued: 'euiNotificationBadge--subdued'
};
export var COLORS = keysOf(colorToClassMap);
var sizeToClassNameMap = {
  s: null,
  m: 'euiNotificationBadge--medium'
};
export var SIZES = keysOf(sizeToClassNameMap);
export var EuiNotificationBadge = function EuiNotificationBadge(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 's' : _ref$size,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'accent' : _ref$color,
      rest = _objectWithoutProperties(_ref, ["children", "className", "size", "color"]);

  var classes = classNames('euiNotificationBadge', sizeToClassNameMap[size], colorToClassMap[color], className);
  return ___EmotionJSX("span", _extends({
    className: classes
  }, rest), children);
};