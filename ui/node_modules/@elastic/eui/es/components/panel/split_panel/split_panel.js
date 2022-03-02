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
import { EuiPanel } from '../panel';
import { useIsWithinBreakpoints } from '../../../services/hooks';
import { jsx as ___EmotionJSX } from "@emotion/react";

/**
 * Consumed via `EuiSplitPanel.Inner`.
 * Extends most `EuiPanelProps`.
 */
export var _EuiSplitPanelInner = function _EuiSplitPanelInner(_ref) {
  var children = _ref.children,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["children", "className"]);

  var classes = classNames('euiSplitPanel__inner', className);
  var panelProps = {
    hasShadow: false,
    color: 'transparent',
    borderRadius: 'none',
    hasBorder: false
  };
  return ___EmotionJSX(EuiPanel, _extends({
    element: "div",
    className: classes
  }, panelProps, rest), children);
};
_EuiSplitPanelInner.propTypes = {
  /**
     * Padding for all four sides
     */
  paddingSize: PropTypes.any,

  /**
     * When true the panel will grow in height to match `EuiFlexItem`
     */
  grow: PropTypes.bool,
  panelRef: PropTypes.any,

  /**
     * Background color of the panel;
     * Usually a lightened form of the brand colors
     */
  color: PropTypes.any,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};

/**
 * Consumed via `EuiSplitPanel.Outer`.
 * Extends most `EuiPanelProps`.
 */
export var _EuiSplitPanelOuter = function _EuiSplitPanelOuter(_ref2) {
  var children = _ref2.children,
      className = _ref2.className,
      _ref2$direction = _ref2.direction,
      direction = _ref2$direction === void 0 ? 'column' : _ref2$direction,
      _ref2$responsive = _ref2.responsive,
      responsive = _ref2$responsive === void 0 ? ['xs', 's'] : _ref2$responsive,
      rest = _objectWithoutProperties(_ref2, ["children", "className", "direction", "responsive"]);

  var isResponsive = useIsWithinBreakpoints(responsive, !!responsive);
  var classes = classNames('euiSplitPanel', {
    'euiSplitPanel--row': direction === 'row',
    'euiSplitPanel-isResponsive': isResponsive
  }, className);
  return ___EmotionJSX(EuiPanel, _extends({
    paddingSize: "none",
    grow: false,
    className: classes
  }, rest), children);
};
_EuiSplitPanelOuter.propTypes = {
  /**
     * Any number of _EuiSplitPanelInner components
     */
  children: PropTypes.node,

  /**
     * Changes the flex-direction
     */
  direction: PropTypes.oneOf(["column", "row"]),

  /**
     * Stacks row display on small screens.
     * Remove completely with `false` or provide your own list of breakpoint sizes to stack on.
     */
  responsive: PropTypes.oneOfType([PropTypes.oneOf([false]), PropTypes.arrayOf(PropTypes.oneOf(["xs", "s", "m", "l", "xl"]).isRequired).isRequired])
};
export var EuiSplitPanel = {
  Outer: _EuiSplitPanelOuter,
  Inner: _EuiSplitPanelInner
};