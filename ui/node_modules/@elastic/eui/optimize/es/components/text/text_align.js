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
import { keysOf } from '../common';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var alignmentToClassNameMap = {
  left: 'euiTextAlign--left',
  right: 'euiTextAlign--right',
  center: 'euiTextAlign--center'
};
export var ALIGNMENTS = keysOf(alignmentToClassNameMap);
export var EuiTextAlign = function EuiTextAlign(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$textAlign = _ref.textAlign,
      textAlign = _ref$textAlign === void 0 ? 'left' : _ref$textAlign,
      rest = _objectWithoutProperties(_ref, ["children", "className", "textAlign"]);

  var classes = classNames('euiTextAlign', alignmentToClassNameMap[textAlign], className);
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), children);
};