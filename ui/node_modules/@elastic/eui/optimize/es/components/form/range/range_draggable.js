import _extends from "@babel/runtime/helpers/extends";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
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
import { useMouseMove } from '../../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiRangeDraggable = function EuiRangeDraggable(_ref) {
  var className = _ref.className,
      showTicks = _ref.showTicks,
      lowerPosition = _ref.lowerPosition,
      upperPosition = _ref.upperPosition,
      compressed = _ref.compressed,
      onChange = _ref.onChange,
      min = _ref.min,
      max = _ref.max,
      disabled = _ref.disabled,
      value = _ref.value,
      rest = _objectWithoutProperties(_ref, ["className", "showTicks", "lowerPosition", "upperPosition", "compressed", "onChange", "min", "max", "disabled", "value"]);

  var outerStyle = {
    left: "calc(".concat(lowerPosition, ")"),
    right: "calc(100% - ".concat(upperPosition, " - 16px)")
  };
  var classes = classNames('euiRangeDraggable', {
    'euiRangeDraggable--hasTicks': showTicks,
    'euiRangeDraggable--compressed': compressed,
    'euiRangeDraggable--disabled': disabled
  }, className);

  var handleChange = function handleChange(_ref2, isFirstInteraction) {
    var x = _ref2.x;
    if (disabled) return;
    onChange(x, isFirstInteraction);
  };

  var _useMouseMove = useMouseMove(handleChange),
      _useMouseMove2 = _slicedToArray(_useMouseMove, 2),
      handleMouseDown = _useMouseMove2[0],
      handleInteraction = _useMouseMove2[1];

  var commonProps = {
    className: classes,
    role: 'slider',
    'aria-valuemin': min,
    'aria-valuemax': max,
    'aria-valuenow': value[0],
    'aria-valuetext': "".concat(value[0], ", ").concat(value[1]),
    'aria-disabled': !!disabled,
    tabIndex: !!disabled ? -1 : 0
  };
  return ___EmotionJSX("div", _extends({
    style: outerStyle
  }, commonProps, rest), ___EmotionJSX("div", {
    className: "euiRangeDraggle__inner",
    onMouseDown: handleMouseDown,
    onTouchStart: handleInteraction,
    onTouchMove: handleInteraction
  }));
};