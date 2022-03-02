import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
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
import { jsx as ___EmotionJSX } from "@emotion/react";
export var GROW_SIZES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
export var EuiFlexItem = function EuiFlexItem(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$grow = _ref.grow,
      grow = _ref$grow === void 0 ? true : _ref$grow,
      _ref$component = _ref.component,
      Component = _ref$component === void 0 ? 'div' : _ref$component,
      rest = _objectWithoutProperties(_ref, ["children", "className", "grow", "component"]);

  validateGrowValue(grow);
  var classes = classNames('euiFlexItem', _defineProperty({
    'euiFlexItem--flexGrowZero': !grow
  }, "euiFlexItem--flexGrow".concat(grow), typeof grow === 'number' ? GROW_SIZES.indexOf(grow) >= 0 : undefined), className);
  return (// @ts-ignore difficult to verify `rest` applies to `Component`
    ___EmotionJSX(Component, _extends({
      className: classes
    }, rest), children)
  );
};

function validateGrowValue(value) {
  var validValues = [null, undefined, true, false].concat(GROW_SIZES);

  if (validValues.indexOf(value) === -1) {
    throw new Error("Prop `grow` passed to `EuiFlexItem` must be a boolean or an integer between 1 and 10, received `".concat(value, "`"));
  }
}