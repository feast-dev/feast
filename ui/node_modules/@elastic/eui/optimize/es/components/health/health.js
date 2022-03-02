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
import { EuiIcon } from '../icon';
import { EuiFlexGroup, EuiFlexItem } from '../flex';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  xs: 'euiHealth--textSizeXS',
  s: 'euiHealth--textSizeS',
  m: 'euiHealth--textSizeM',
  inherit: 'euiHealth--textSizeInherit'
};
export var TEXT_SIZES = keysOf(sizeToClassNameMap);
export var EuiHealth = function EuiHealth(_ref) {
  var children = _ref.children,
      className = _ref.className,
      color = _ref.color,
      _ref$textSize = _ref.textSize,
      textSize = _ref$textSize === void 0 ? 's' : _ref$textSize,
      rest = _objectWithoutProperties(_ref, ["children", "className", "color", "textSize"]);

  var classes = classNames('euiHealth', textSize ? sizeToClassNameMap[textSize] : null, className);
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX(EuiFlexGroup, {
    gutterSize: "xs",
    alignItems: "center",
    responsive: false
  }, ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, ___EmotionJSX(EuiIcon, {
    type: "dot",
    color: color
  })), ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, children)));
};