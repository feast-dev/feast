function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import classNames from 'classnames';
import PropTypes from "prop-types";
import React from 'react';
import { EuiScreenReaderOnly } from '../accessibility';
import { keysOf } from '../common';
import { EuiIcon } from '../icon';
import { useI18nCompleteStep, useI18nDisabledStep, useI18nErrorsStep, useI18nIncompleteStep, useI18nStep, useI18nWarningStep, useI18nLoadingStep, useI18nCurrentStep } from './step_strings';
import { EuiLoadingSpinner } from '../loading';
import { jsx as ___EmotionJSX } from "@emotion/react";
var statusToClassNameMap = {
  incomplete: 'euiStepNumber--incomplete',
  disabled: 'euiStepNumber--disabled',
  loading: 'euiStepNumber--loading',
  warning: 'euiStepNumber--warning',
  danger: 'euiStepNumber--danger',
  complete: 'euiStepNumber--complete',
  current: null // Current displays the same as the default (undefined)

};
export var STATUS = keysOf(statusToClassNameMap);
export var EuiStepNumber = function EuiStepNumber(_ref) {
  var className = _ref.className,
      status = _ref.status,
      number = _ref.number,
      isHollow = _ref.isHollow,
      titleSize = _ref.titleSize,
      rest = _objectWithoutProperties(_ref, ["className", "status", "number", "isHollow", "titleSize"]);

  var stepAriaLabel = useI18nStep({
    number: number
  });
  var completeAriaLabel = useI18nCompleteStep({
    number: number
  });
  var warningAriaLabel = useI18nWarningStep({
    number: number
  });
  var errorsAriaLabel = useI18nErrorsStep({
    number: number
  });
  var incompleteAriaLabel = useI18nIncompleteStep({
    number: number
  });
  var disabledAriaLabel = useI18nDisabledStep({
    number: number
  });
  var loadingAriaLabel = useI18nLoadingStep({
    number: number
  });
  var currentAriaLabel = useI18nCurrentStep({
    number: number
  });
  var classes = classNames('euiStepNumber', status ? statusToClassNameMap[status] : undefined, {
    'euiStepNumber-isHollow': isHollow
  }, className);
  var iconSize = titleSize === 'xs' ? 's' : 'm';
  var screenReaderText = stepAriaLabel;
  if (status === 'incomplete') screenReaderText = incompleteAriaLabel;else if (status === 'disabled') screenReaderText = disabledAriaLabel;else if (status === 'loading') screenReaderText = loadingAriaLabel;else if (status === 'current') screenReaderText = currentAriaLabel;

  var numberOrIcon = ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, screenReaderText)), ___EmotionJSX("span", {
    className: "euiStepNumber__number",
    "aria-hidden": "true"
  }, number));

  if (status === 'complete') {
    numberOrIcon = ___EmotionJSX(EuiIcon, {
      type: "check",
      className: "euiStepNumber__icon",
      size: iconSize,
      "aria-label": completeAriaLabel
    });
  } else if (status === 'warning') {
    numberOrIcon = ___EmotionJSX(EuiIcon, {
      type: "alert",
      className: "euiStepNumber__icon",
      size: iconSize,
      "aria-label": warningAriaLabel
    });
  } else if (status === 'danger') {
    numberOrIcon = ___EmotionJSX(EuiIcon, {
      type: "cross",
      className: "euiStepNumber__icon",
      size: iconSize,
      "aria-label": errorsAriaLabel
    });
  } else if (status === 'loading') {
    numberOrIcon = ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, screenReaderText)), ___EmotionJSX(EuiLoadingSpinner, {
      className: "euiStepNumber__loader",
      size: iconSize === 's' ? 'l' : 'xl'
    }));
  }

  return ___EmotionJSX("span", _extends({
    className: classes
  }, rest), numberOrIcon);
};
EuiStepNumber.propTypes = {
  /**
     * May replace the number provided in props.number with alternate styling
     */
  status: PropTypes.any,
  number: PropTypes.number,

  /**
     * **DEPRECATED IN AMSTERDAM**
     * Uses a border and removes the step number.
     */
  isHollow: PropTypes.bool,

  /**
     * Title sizing equivalent to EuiTitle, but only `m`, `s` and `xs`. Defaults to `s`
     */
  titleSize: PropTypes.any,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};