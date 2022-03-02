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
export var EuiRangeThumb = function EuiRangeThumb(_ref) {
  var className = _ref.className,
      min = _ref.min,
      max = _ref.max,
      value = _ref.value,
      disabled = _ref.disabled,
      showInput = _ref.showInput,
      showTicks = _ref.showTicks,
      onClick = _ref.onClick,
      onMouseDown = _ref.onMouseDown,
      tabIndex = _ref.tabIndex,
      rest = _objectWithoutProperties(_ref, ["className", "min", "max", "value", "disabled", "showInput", "showTicks", "onClick", "onMouseDown", "tabIndex"]);

  var classes = classNames('euiRangeThumb', {
    'euiRangeThumb--hasTicks': showTicks
  }, className);
  var commonAttrs = {
    className: classes,
    role: 'slider',
    'aria-valuemin': min,
    'aria-valuemax': max,
    'aria-valuenow': Number(value),
    'aria-disabled': !!disabled,
    tabIndex: showInput || !!disabled ? -1 : tabIndex || 0
  };
  return onClick || onMouseDown ? ___EmotionJSX("button", _extends({
    type: "button",
    onClick: onClick,
    onMouseDown: onMouseDown,
    disabled: disabled
  }, commonAttrs, rest)) : ___EmotionJSX("div", _extends({}, commonAttrs, rest));
};