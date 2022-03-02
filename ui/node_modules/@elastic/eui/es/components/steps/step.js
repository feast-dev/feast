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
import React, { createElement } from 'react';
import { EuiTitle } from '../title';
import { EuiStepNumber } from './step_number';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiStep = function EuiStep(_ref) {
  var className = _ref.className,
      children = _ref.children,
      _ref$headingElement = _ref.headingElement,
      headingElement = _ref$headingElement === void 0 ? 'p' : _ref$headingElement,
      _ref$step = _ref.step,
      step = _ref$step === void 0 ? 1 : _ref$step,
      title = _ref.title,
      _ref$titleSize = _ref.titleSize,
      titleSize = _ref$titleSize === void 0 ? 's' : _ref$titleSize,
      status = _ref.status,
      rest = _objectWithoutProperties(_ref, ["className", "children", "headingElement", "step", "title", "titleSize", "status"]);

  var classes = classNames('euiStep', {
    'euiStep--small': titleSize === 'xs',
    'euiStep-isDisabled': status === 'disabled'
  }, className);
  var numberClasses = classNames('euiStep__circle', {
    'euiStepNumber--small': titleSize === 'xs'
  });
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX("div", {
    className: "euiStep__titleWrapper"
  }, ___EmotionJSX(EuiStepNumber, {
    className: numberClasses,
    number: step,
    status: status,
    titleSize: titleSize,
    isHollow: status === 'incomplete'
  }), ___EmotionJSX(EuiTitle, {
    size: titleSize,
    className: "euiStep__title"
  }, /*#__PURE__*/createElement(headingElement, null, title))), ___EmotionJSX("div", {
    className: "euiStep__content"
  }, children));
};
EuiStep.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * ReactNode to render as this component's content
     */
  children: PropTypes.node.isRequired,

  /**
     * The HTML tag used for the title
     */
  headingElement: PropTypes.string,

  /**
     * The number of the step in the list of steps
     */
  step: PropTypes.number,
  title: PropTypes.string.isRequired,

  /**
     * May replace the number provided in props.step with alternate styling.
     */
  status: PropTypes.any,

  /**
     * Title sizing equivalent to EuiTitle, but only `m`, `s` and `xs`. Defaults to `s`
     */
  titleSize: PropTypes.any
};