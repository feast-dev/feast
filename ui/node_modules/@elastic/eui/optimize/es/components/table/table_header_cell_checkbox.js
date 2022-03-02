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
import { resolveWidthAsStyle } from './utils';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiTableHeaderCellCheckbox = function EuiTableHeaderCellCheckbox(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$scope = _ref.scope,
      scope = _ref$scope === void 0 ? 'col' : _ref$scope,
      style = _ref.style,
      width = _ref.width,
      rest = _objectWithoutProperties(_ref, ["children", "className", "scope", "style", "width"]);

  var classes = classNames('euiTableHeaderCellCheckbox', className);
  var styleObj = resolveWidthAsStyle(style, width);
  return ___EmotionJSX("th", _extends({
    className: classes,
    scope: scope,
    style: styleObj
  }, rest), ___EmotionJSX("div", {
    className: "euiTableCellContent"
  }, children));
};