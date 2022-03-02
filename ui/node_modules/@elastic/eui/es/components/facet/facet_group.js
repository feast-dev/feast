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
EuiFacetGroup.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Vertically in a column, or horizontally in one wrapping line
       */
  layout: PropTypes.oneOf(["vertical", "horizontal"]),

  /**
       * Distance between facet buttons.
       * Horizontal layout always adds more distance horizontally between buttons.
       */
  gutterSize: PropTypes.oneOf(["none", "s", "m", "l"])
};