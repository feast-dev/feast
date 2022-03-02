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
import { EuiContextMenuItem } from '../../context_menu';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiTableSortMobileItem = function EuiTableSortMobileItem(_ref) {
  var children = _ref.children,
      onSort = _ref.onSort,
      isSorted = _ref.isSorted,
      isSortAscending = _ref.isSortAscending,
      className = _ref.className,
      ariaLabel = _ref.ariaLabel,
      rest = _objectWithoutProperties(_ref, ["children", "onSort", "isSorted", "isSortAscending", "className", "ariaLabel"]);

  var sortIcon = 'empty';

  if (isSorted) {
    sortIcon = isSortAscending ? 'sortUp' : 'sortDown';
  }

  var buttonClasses = classNames('euiTableSortMobileItem', className, {
    'euiTableSortMobileItem-isSorted': isSorted
  });
  var columnTitle = ariaLabel ? ariaLabel : children;
  var statefulAriaLabel = "Sort ".concat(columnTitle, " ").concat(isSortAscending ? 'descending' : 'ascending');
  return ___EmotionJSX(EuiContextMenuItem, _extends({
    className: buttonClasses,
    icon: sortIcon,
    onClick: onSort,
    "aria-label": statefulAriaLabel
  }, rest), children);
};