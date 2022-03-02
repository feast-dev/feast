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
export var EuiSplitPanel = {
  Outer: _EuiSplitPanelOuter,
  Inner: _EuiSplitPanelInner
};