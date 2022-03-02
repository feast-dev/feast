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
var paddingSizeToClassNameMap = {
  none: null,
  s: 'euiPageSideBar--paddingSmall',
  m: 'euiPageSideBar--paddingMedium',
  l: 'euiPageSideBar--paddingLarge'
};
export var PADDING_SIZES = keysOf(paddingSizeToClassNameMap);
export var EuiPageSideBar = function EuiPageSideBar(_ref) {
  var children = _ref.children,
      className = _ref.className,
      sticky = _ref.sticky,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'none' : _ref$paddingSize,
      rest = _objectWithoutProperties(_ref, ["children", "className", "sticky", "paddingSize"]);

  var classes = classNames('euiPageSideBar', paddingSizeToClassNameMap[paddingSize], {
    'euiPageSideBar--sticky': sticky
  }, className);
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), children);
};