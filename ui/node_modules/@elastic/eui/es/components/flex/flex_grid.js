function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import PropTypes from "prop-types";
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
EuiFlexGrid.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * ReactNode to render as this component's content
     */
  children: PropTypes.node,

  /**
     * Number of columns `1-4`, pass `0` for normal display
     */
  columns: PropTypes.oneOf([0, 1, 2, 3, 4]),

  /**
     * Flex layouts default to left-right then top-down (`row`).
     * Change this prop to `column` to create a top-down then left-right display.
     * Only works with column count of `1-4`.
     */
  direction: PropTypes.oneOf(["row", "column"]),

  /**
     * Space between flex items
     */
  gutterSize: PropTypes.oneOf(["none", "s", "m", "l", "xl"]),

  /**
     * Force each item to be display block on smaller screens
     */
  responsive: PropTypes.bool,

  /**
     * The tag to render
     */
  component: PropTypes.any
};