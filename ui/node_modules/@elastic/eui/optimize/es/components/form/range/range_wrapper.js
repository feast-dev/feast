import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { forwardRef } from 'react';
import classNames from 'classnames';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiRangeWrapper = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var children = _ref.children,
      className = _ref.className,
      fullWidth = _ref.fullWidth,
      compressed = _ref.compressed,
      rest = _objectWithoutProperties(_ref, ["children", "className", "fullWidth", "compressed"]);

  var classes = classNames('euiRangeWrapper', {
    'euiRangeWrapper--fullWidth': fullWidth,
    'euiRangeWrapper--compressed': compressed
  }, className);
  return ___EmotionJSX("div", _extends({
    className: classes,
    ref: ref
  }, rest), children);
});
EuiRangeWrapper.displayName = 'EuiRangeWrapper';