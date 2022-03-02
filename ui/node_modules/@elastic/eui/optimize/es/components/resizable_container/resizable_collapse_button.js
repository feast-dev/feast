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
import { EuiButtonIcon } from '../button';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiResizableCollapseButton = function EuiResizableCollapseButton(_ref) {
  var className = _ref.className,
      externalPosition = _ref.externalPosition,
      _ref$internalPosition = _ref.internalPosition,
      internalPosition = _ref$internalPosition === void 0 ? 'middle' : _ref$internalPosition,
      _ref$direction = _ref.direction,
      direction = _ref$direction === void 0 ? 'horizontal' : _ref$direction,
      isVisible = _ref.isVisible,
      isCollapsed = _ref.isCollapsed,
      rest = _objectWithoutProperties(_ref, ["className", "externalPosition", "internalPosition", "direction", "isVisible", "isCollapsed"]);

  var isHorizontal = direction === 'horizontal';
  var classes = classNames('euiResizableToggleButton', "euiResizableToggleButton--".concat(direction), "euiResizableToggleButton--".concat(externalPosition), "euiResizableToggleButton--".concat(internalPosition), {
    'euiResizableToggleButton-isVisible': isVisible,
    'euiResizableToggleButton-isCollapsed': isCollapsed
  }, className); // Default to simiple grab icon in case there is no externalPosition specified

  var COLLAPSED_ICON = isHorizontal ? 'grab' : 'grabHorizontal';
  var NOT_COLLAPSED_ICON = isHorizontal ? 'grab' : 'grabHorizontal';

  switch (externalPosition) {
    case 'before':
      COLLAPSED_ICON = isHorizontal ? 'menuLeft' : 'menuUp';
      NOT_COLLAPSED_ICON = isHorizontal ? 'menuRight' : 'menuDown';
      break;

    case 'after':
      COLLAPSED_ICON = isHorizontal ? 'menuRight' : 'menuDown';
      NOT_COLLAPSED_ICON = isHorizontal ? 'menuLeft' : 'menuUp';
      break;
  }

  return ___EmotionJSX(EuiButtonIcon, _extends({
    display: isCollapsed ? 'empty' : 'fill',
    color: isCollapsed ? 'text' : 'ghost'
  }, rest, {
    className: classes,
    iconType: isCollapsed ? COLLAPSED_ICON : NOT_COLLAPSED_ICON
  }));
};