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
var directionToClassNameMap = {
  row: null,
  column: 'euiFlexGrid--directionColumn'
};
export var DIRECTIONS = keysOf(directionToClassNameMap);
var gutterSizeToClassNameMap = {
  none: 'euiFlexGrid--gutterNone',
  s: 'euiFlexGrid--gutterSmall',
  m: 'euiFlexGrid--gutterMedium',
  l: 'euiFlexGrid--gutterLarge',
  xl: 'euiFlexGrid--gutterXLarge'
};
export var GUTTER_SIZES = keysOf(gutterSizeToClassNameMap);
var columnsToClassNameMap = {
  0: 'euiFlexGrid--wrap',
  1: 'euiFlexGrid--single',
  2: 'euiFlexGrid--halves',
  3: 'euiFlexGrid--thirds',
  4: 'euiFlexGrid--fourths'
};
export var COLUMNS = Object.keys(columnsToClassNameMap).map(function (columns) {
  return parseInt(columns, 10);
});
export var EuiFlexGrid = function EuiFlexGrid(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$gutterSize = _ref.gutterSize,
      gutterSize = _ref$gutterSize === void 0 ? 'l' : _ref$gutterSize,
      _ref$direction = _ref.direction,
      direction = _ref$direction === void 0 ? 'row' : _ref$direction,
      _ref$responsive = _ref.responsive,
      responsive = _ref$responsive === void 0 ? true : _ref$responsive,
      _ref$columns = _ref.columns,
      columns = _ref$columns === void 0 ? 0 : _ref$columns,
      _ref$component = _ref.component,
      Component = _ref$component === void 0 ? 'div' : _ref$component,
      rest = _objectWithoutProperties(_ref, ["children", "className", "gutterSize", "direction", "responsive", "columns", "component"]);

  var classes = classNames('euiFlexGrid', gutterSize ? gutterSizeToClassNameMap[gutterSize] : undefined, columns != null ? columnsToClassNameMap[columns] : undefined, direction ? directionToClassNameMap[direction] : undefined, {
    'euiFlexGrid--responsive': responsive
  }, className);
  return (// @ts-ignore difficult to verify `rest` applies to `Component`
    ___EmotionJSX(Component, _extends({
      className: classes
    }, rest), children)
  );
};