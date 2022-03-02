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
export var EuiRangeSlider = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var className = _ref.className,
      disabled = _ref.disabled,
      id = _ref.id,
      max = _ref.max,
      min = _ref.min,
      name = _ref.name,
      step = _ref.step,
      onChange = _ref.onChange,
      tabIndex = _ref.tabIndex,
      value = _ref.value,
      style = _ref.style,
      showTicks = _ref.showTicks,
      showRange = _ref.showRange,
      hasFocus = _ref.hasFocus,
      compressed = _ref.compressed,
      rest = _objectWithoutProperties(_ref, ["className", "disabled", "id", "max", "min", "name", "step", "onChange", "tabIndex", "value", "style", "showTicks", "showRange", "hasFocus", "compressed"]);

  var classes = classNames('euiRangeSlider', {
    'euiRangeSlider--hasTicks': showTicks,
    'euiRangeSlider--hasFocus': hasFocus,
    'euiRangeSlider--hasRange': showRange,
    'euiRangeSlider--compressed': compressed
  }, className);
  return ___EmotionJSX("input", _extends({
    ref: ref,
    type: "range",
    id: id,
    name: name,
    className: classes,
    min: min,
    max: max,
    step: step,
    value: value,
    disabled: disabled,
    onChange: onChange,
    style: style,
    tabIndex: tabIndex
  }, rest));
});
EuiRangeSlider.displayName = 'EuiRangeSlider';