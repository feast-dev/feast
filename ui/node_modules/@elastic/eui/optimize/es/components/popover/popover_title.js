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
var paddingSizeToClassNameMap = {
  none: 'euiPopoverTitle--paddingNone',
  s: 'euiPopoverTitle--paddingSmall',
  m: 'euiPopoverTitle--paddingMedium',
  l: 'euiPopoverTitle--paddingLarge'
};
export var PADDING_SIZES = keysOf(paddingSizeToClassNameMap);
export var EuiPopoverTitle = function EuiPopoverTitle(_ref) {
  var children = _ref.children,
      className = _ref.className,
      paddingSize = _ref.paddingSize,
      rest = _objectWithoutProperties(_ref, ["children", "className", "paddingSize"]);

  var classes = classNames('euiPopoverTitle', paddingSize ? paddingSizeToClassNameMap[paddingSize] : null, className);
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), children);
};