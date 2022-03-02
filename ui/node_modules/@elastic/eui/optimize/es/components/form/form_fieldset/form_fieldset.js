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
import { EuiFormLegend } from './form_legend';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFormFieldset = function EuiFormFieldset(_ref) {
  var children = _ref.children,
      className = _ref.className,
      legend = _ref.legend,
      rest = _objectWithoutProperties(_ref, ["children", "className", "legend"]);

  var legendDisplay = !!legend && ___EmotionJSX(EuiFormLegend, legend);

  return ___EmotionJSX("fieldset", _extends({
    className: className
  }, rest), legendDisplay, children);
};