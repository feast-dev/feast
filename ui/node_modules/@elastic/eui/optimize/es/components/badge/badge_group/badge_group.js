import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { forwardRef } from 'react';
import classNames from 'classnames';
import { keysOf } from '../../common';
import { jsx as ___EmotionJSX } from "@emotion/react";
var gutterSizeToClassNameMap = {
  none: null,
  xs: 'euiBadgeGroup--gutterExtraSmall',
  s: 'euiBadgeGroup--gutterSmall'
};
export var GUTTER_SIZES = keysOf(gutterSizeToClassNameMap);
export var EuiBadgeGroup = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$gutterSize = _ref.gutterSize,
      gutterSize = _ref$gutterSize === void 0 ? 'xs' : _ref$gutterSize,
      rest = _objectWithoutProperties(_ref, ["children", "className", "gutterSize"]);

  var classes = classNames('euiBadgeGroup', gutterSizeToClassNameMap[gutterSize], className);
  return ___EmotionJSX("div", _extends({
    className: classes,
    ref: ref
  }, rest), React.Children.map(children, function (child) {
    return ___EmotionJSX("span", {
      className: "euiBadgeGroup__item"
    }, child);
  }));
});
EuiBadgeGroup.displayName = 'EuiBadgeGroup';