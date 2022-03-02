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
import { EuiFlexGroup } from '../flex';
import { jsx as ___EmotionJSX } from "@emotion/react";
var layoutToClassNameMap = {
  vertical: 'euiFacetGroup--vertical',
  horizontal: 'euiFacetGroup--horizontal'
};
export var LAYOUTS = keysOf(layoutToClassNameMap);
var gutterSizeToClassNameMap = {
  none: 'euiFacetGroup--gutterNone',
  s: 'euiFacetGroup--gutterSmall',
  m: 'euiFacetGroup--gutterMedium',
  l: 'euiFacetGroup--gutterLarge'
};
export var GUTTER_SIZES = keysOf(gutterSizeToClassNameMap);
export var EuiFacetGroup = function EuiFacetGroup(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$layout = _ref.layout,
      layout = _ref$layout === void 0 ? 'vertical' : _ref$layout,
      _ref$gutterSize = _ref.gutterSize,
      gutterSize = _ref$gutterSize === void 0 ? 'm' : _ref$gutterSize,
      rest = _objectWithoutProperties(_ref, ["children", "className", "layout", "gutterSize"]);

  var classes = classNames('euiFacetGroup', layoutToClassNameMap[layout], gutterSizeToClassNameMap[gutterSize], className);
  var direction = layout === 'vertical' ? 'column' : 'row';
  var wrap = layout === 'vertical' ? false : true;
  return ___EmotionJSX(EuiFlexGroup, _extends({
    className: classes,
    direction: direction,
    wrap: wrap,
    gutterSize: "none"
  }, rest), children);
};