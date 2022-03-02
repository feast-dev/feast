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
import { jsx as ___EmotionJSX } from "@emotion/react";
var tableLayoutToClassMap = {
  fixed: null,
  auto: 'euiTable--auto'
};
export var EuiTable = function EuiTable(_ref) {
  var children = _ref.children,
      className = _ref.className,
      compressed = _ref.compressed,
      _ref$tableLayout = _ref.tableLayout,
      tableLayout = _ref$tableLayout === void 0 ? 'fixed' : _ref$tableLayout,
      _ref$responsive = _ref.responsive,
      responsive = _ref$responsive === void 0 ? true : _ref$responsive,
      rest = _objectWithoutProperties(_ref, ["children", "className", "compressed", "tableLayout", "responsive"]);

  var classes = classNames('euiTable', className, {
    'euiTable--compressed': compressed,
    'euiTable--responsive': responsive
  }, tableLayoutToClassMap[tableLayout]);
  return ___EmotionJSX("table", _extends({
    tabIndex: -1,
    className: classes
  }, rest), children);
};