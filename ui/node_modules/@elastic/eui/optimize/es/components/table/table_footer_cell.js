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
import { LEFT_ALIGNMENT, RIGHT_ALIGNMENT, CENTER_ALIGNMENT } from '../../services';
import { resolveWidthAsStyle } from './utils';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiTableFooterCell = function EuiTableFooterCell(_ref) {
  var children = _ref.children,
      _ref$align = _ref.align,
      align = _ref$align === void 0 ? LEFT_ALIGNMENT : _ref$align,
      className = _ref.className,
      width = _ref.width,
      style = _ref.style,
      rest = _objectWithoutProperties(_ref, ["children", "align", "className", "width", "style"]);

  var classes = classNames('euiTableFooterCell', className);
  var contentClasses = classNames('euiTableCellContent', className, {
    'euiTableCellContent--alignRight': align === RIGHT_ALIGNMENT,
    'euiTableCellContent--alignCenter': align === CENTER_ALIGNMENT
  });
  var styleObj = resolveWidthAsStyle(style, width);
  return ___EmotionJSX("td", _extends({
    className: classes,
    style: styleObj
  }, rest), ___EmotionJSX("div", {
    className: contentClasses
  }, ___EmotionJSX("span", {
    className: "euiTableCellContent__text"
  }, children)));
};