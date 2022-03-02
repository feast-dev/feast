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
import { keys } from '../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiTableRow = function EuiTableRow(_ref) {
  var children = _ref.children,
      className = _ref.className,
      isSelected = _ref.isSelected,
      isSelectable = _ref.isSelectable,
      hasActions = _ref.hasActions,
      isExpandedRow = _ref.isExpandedRow,
      isExpandable = _ref.isExpandable,
      onClick = _ref.onClick,
      rest = _objectWithoutProperties(_ref, ["children", "className", "isSelected", "isSelectable", "hasActions", "isExpandedRow", "isExpandable", "onClick"]);

  var classes = classNames('euiTableRow', className, {
    'euiTableRow-isSelectable': isSelectable,
    'euiTableRow-isSelected': isSelected,
    'euiTableRow-hasActions': hasActions,
    'euiTableRow-isExpandedRow': isExpandedRow,
    'euiTableRow-isExpandable': isExpandable,
    'euiTableRow-isClickable': onClick
  });

  if (!onClick) {
    return ___EmotionJSX("tr", _extends({
      className: classes
    }, rest), children);
  }

  var onKeyDown = function onKeyDown(event) {
    // Prevent a scroll from occurring if the user has hit space.
    if (event.key === keys.SPACE) event.preventDefault();
  };

  var onKeyUp = function onKeyUp(event) {
    // Support keyboard accessibility by emulating mouse click on ENTER or SPACE keypress.
    if (event.key === keys.ENTER || event.key === keys.SPACE) {
      onClick(event);
    }
  };

  return ___EmotionJSX("tr", _extends({
    className: classes,
    onClick: onClick,
    onKeyDown: onKeyDown,
    onKeyUp: onKeyUp,
    tabIndex: 0
  }, rest), children);
};