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
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiTableHeader = function EuiTableHeader(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$wrapWithTableRow = _ref.wrapWithTableRow,
      wrapWithTableRow = _ref$wrapWithTableRow === void 0 ? true : _ref$wrapWithTableRow,
      rest = _objectWithoutProperties(_ref, ["children", "className", "wrapWithTableRow"]);

  return ___EmotionJSX("thead", _extends({
    className: className
  }, rest), wrapWithTableRow ? ___EmotionJSX("tr", null, children) : children);
};