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
import { EuiScreenReaderOnly } from '../../accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFormLegend = function EuiFormLegend(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$display = _ref.display,
      display = _ref$display === void 0 ? 'visible' : _ref$display,
      compressed = _ref.compressed,
      rest = _objectWithoutProperties(_ref, ["children", "className", "display", "compressed"]);

  var isLegendHidden = display === 'hidden';
  var classes = classNames('euiFormLegend', {
    'euiFormLegend-isHidden': isLegendHidden,
    'euiFormLegend--compressed': compressed
  }, className);
  return ___EmotionJSX("legend", _extends({
    className: classes
  }, rest), isLegendHidden ? ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, children)) : children);
};