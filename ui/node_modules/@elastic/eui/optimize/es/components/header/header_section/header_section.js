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
import { jsx as ___EmotionJSX } from "@emotion/react";
var sideToClassNameMap = {
  left: 'euiHeaderSection--left',
  right: 'euiHeaderSection--right'
};
export var EuiHeaderSection = function EuiHeaderSection(_ref) {
  var _ref$side = _ref.side,
      side = _ref$side === void 0 ? 'left' : _ref$side,
      children = _ref.children,
      className = _ref.className,
      _ref$grow = _ref.grow,
      grow = _ref$grow === void 0 ? false : _ref$grow,
      rest = _objectWithoutProperties(_ref, ["side", "children", "className", "grow"]);

  var classes = classNames('euiHeaderSection', {
    'euiHeaderSection--grow': grow,
    'euiHeaderSection--dontGrow': !grow
  }, sideToClassNameMap[side], className);
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), children);
};