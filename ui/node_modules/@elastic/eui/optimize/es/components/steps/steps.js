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
import { EuiStep } from './step';
import { jsx as ___EmotionJSX } from "@emotion/react";

function renderSteps(steps, firstStepNumber, headingElement, titleSize) {
  return steps.map(function (step, index) {
    var className = step.className,
        children = step.children,
        title = step.title,
        status = step.status,
        rest = _objectWithoutProperties(step, ["className", "children", "title", "status"]);

    return ___EmotionJSX(EuiStep, _extends({
      className: className,
      key: index,
      headingElement: headingElement,
      step: firstStepNumber + index,
      title: title,
      titleSize: titleSize,
      status: status
    }, rest), children);
  });
}

export var EuiSteps = function EuiSteps(_ref) {
  var className = _ref.className,
      _ref$firstStepNumber = _ref.firstStepNumber,
      firstStepNumber = _ref$firstStepNumber === void 0 ? 1 : _ref$firstStepNumber,
      _ref$headingElement = _ref.headingElement,
      headingElement = _ref$headingElement === void 0 ? 'p' : _ref$headingElement,
      titleSize = _ref.titleSize,
      steps = _ref.steps,
      rest = _objectWithoutProperties(_ref, ["className", "firstStepNumber", "headingElement", "titleSize", "steps"]);

  var classes = classNames('euiSteps', className);
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), renderSteps(steps, firstStepNumber, headingElement, titleSize));
};