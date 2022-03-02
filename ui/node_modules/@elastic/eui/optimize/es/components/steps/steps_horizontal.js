import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import classNames from 'classnames';
import React from 'react';
import { EuiStepHorizontal } from './step_horizontal';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiStepsHorizontal = function EuiStepsHorizontal(_ref) {
  var className = _ref.className,
      steps = _ref.steps,
      rest = _objectWithoutProperties(_ref, ["className", "steps"]);

  var classes = classNames('euiStepsHorizontal', className);
  return ___EmotionJSX("ol", _extends({
    className: classes
  }, rest), steps.map(function (stepProps, index) {
    var isCurrent = stepProps.isSelected || stepProps.status === 'current' ? {
      'aria-current': 'step'
    } : {};
    return ___EmotionJSX("li", _extends({
      key: index,
      className: "euiStepHorizontal__item"
    }, isCurrent), ___EmotionJSX(EuiStepHorizontal, _extends({
      step: index + 1
    }, stepProps)));
  }));
};