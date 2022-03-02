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
EuiTableRow.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Indicates if the table has a single column of checkboxes for selecting
     * rows (affects mobile only)
     */
  isSelectable: PropTypes.bool,

  /**
     * Indicates the current row has been selected
     */
  isSelected: PropTypes.bool,

  /**
     * Indicates if the table has a dedicated column for icon-only actions
     * (affects mobile only)
     */
  hasActions: PropTypes.bool,

  /**
     * Indicates if the row will have an expanded row
     */
  isExpandable: PropTypes.bool,

  /**
     * Indicates if the row will be the expanded row
     */
  isExpandedRow: PropTypes.bool,
  onClick: PropTypes.any
};