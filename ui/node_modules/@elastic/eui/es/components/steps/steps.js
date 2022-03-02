function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import PropTypes from "prop-types";

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

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
EuiSteps.propTypes = {
  /**
     * An array of `EuiStep` objects excluding the `step` prop
     */
  steps: PropTypes.arrayOf(PropTypes.any.isRequired).isRequired,

  /**
     * The number the steps should begin from
     */
  firstStepNumber: PropTypes.number,

  /**
     * The HTML tag used for the title
     */
  headingElement: PropTypes.string,

  /**
     * Title sizing equivalent to EuiTitle, but only `m`, `s` and `xs`. Defaults to `s`
     */
  titleSize: PropTypes.any,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};