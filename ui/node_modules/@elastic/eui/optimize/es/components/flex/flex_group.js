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
import { keysOf } from '../common';
import { jsx as ___EmotionJSX } from "@emotion/react";
var gutterSizeToClassNameMap = {
  none: null,
  xs: 'euiFlexGroup--gutterExtraSmall',
  s: 'euiFlexGroup--gutterSmall',
  m: 'euiFlexGroup--gutterMedium',
  l: 'euiFlexGroup--gutterLarge',
  xl: 'euiFlexGroup--gutterExtraLarge'
};
export var GUTTER_SIZES = keysOf(gutterSizeToClassNameMap);
var alignItemsToClassNameMap = {
  stretch: null,
  flexStart: 'euiFlexGroup--alignItemsFlexStart',
  flexEnd: 'euiFlexGroup--alignItemsFlexEnd',
  center: 'euiFlexGroup--alignItemsCenter',
  baseline: 'euiFlexGroup--alignItemsBaseline'
};
export var ALIGN_ITEMS = keysOf(alignItemsToClassNameMap);
var justifyContentToClassNameMap = {
  flexStart: null,
  flexEnd: 'euiFlexGroup--justifyContentFlexEnd',
  center: 'euiFlexGroup--justifyContentCenter',
  spaceBetween: 'euiFlexGroup--justifyContentSpaceBetween',
  spaceAround: 'euiFlexGroup--justifyContentSpaceAround',
  spaceEvenly: 'euiFlexGroup--justifyContentSpaceEvenly'
};
export var JUSTIFY_CONTENTS = keysOf(justifyContentToClassNameMap);
var directionToClassNameMap = {
  row: 'euiFlexGroup--directionRow',
  rowReverse: 'euiFlexGroup--directionRowReverse',
  column: 'euiFlexGroup--directionColumn',
  columnReverse: 'euiFlexGroup--directionColumnReverse'
};
export var DIRECTIONS = keysOf(directionToClassNameMap);

var isValidElement = function isValidElement(component) {
  return ['div', 'span'].includes(component);
};

export var EuiFlexGroup = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$gutterSize = _ref.gutterSize,
      gutterSize = _ref$gutterSize === void 0 ? 'l' : _ref$gutterSize,
      _ref$alignItems = _ref.alignItems,
      alignItems = _ref$alignItems === void 0 ? 'stretch' : _ref$alignItems,
      _ref$responsive = _ref.responsive,
      responsive = _ref$responsive === void 0 ? true : _ref$responsive,
      _ref$justifyContent = _ref.justifyContent,
      justifyContent = _ref$justifyContent === void 0 ? 'flexStart' : _ref$justifyContent,
      _ref$direction = _ref.direction,
      direction = _ref$direction === void 0 ? 'row' : _ref$direction,
      _ref$wrap = _ref.wrap,
      wrap = _ref$wrap === void 0 ? false : _ref$wrap,
      _ref$component = _ref.component,
      component = _ref$component === void 0 ? 'div' : _ref$component,
      rest = _objectWithoutProperties(_ref, ["children", "className", "gutterSize", "alignItems", "responsive", "justifyContent", "direction", "wrap", "component"]);

  var classes = classNames('euiFlexGroup', gutterSizeToClassNameMap[gutterSize], alignItemsToClassNameMap[alignItems], justifyContentToClassNameMap[justifyContent], directionToClassNameMap[direction], {
    'euiFlexGroup--responsive': responsive,
    'euiFlexGroup--wrap': wrap
  }, className);

  if (!isValidElement(component)) {
    throw new Error("".concat(component, " is not a valid element type. Use `div` or `span`."));
  }

  return component === 'span' ? ___EmotionJSX("span", _extends({
    className: classes,
    ref: ref
  }, rest), children) : ___EmotionJSX("div", _extends({
    className: classes,
    ref: ref
  }, rest), children);
});
EuiFlexGroup.displayName = 'EuiFlexGroup';