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