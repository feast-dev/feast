import _extends from "@babel/runtime/helpers/extends";
import _typeof from "@babel/runtime/helpers/typeof";
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
import { EuiFormLabel } from '../form/form_label/form_label';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiKeyPadMenu = function EuiKeyPadMenu(_ref) {
  var children = _ref.children,
      className = _ref.className,
      checkable = _ref.checkable,
      rest = _objectWithoutProperties(_ref, ["children", "className", "checkable"]);

  var classes = classNames('euiKeyPadMenu', className);
  var legend = _typeof(checkable) === 'object' && checkable.legend ? ___EmotionJSX(EuiFormLabel, _extends({}, checkable.legendProps, {
    type: "legend"
  }), checkable.legend) : undefined;
  return checkable ? ___EmotionJSX("fieldset", _extends({
    className: classes,
    "aria-label": _typeof(checkable) === 'object' ? checkable.ariaLegend : undefined
  }, rest), legend, children) : ___EmotionJSX("ul", _extends({
    className: classes
  }, rest), React.Children.map(children, function (child) {
    return ___EmotionJSX("li", null, child);
  }));
};