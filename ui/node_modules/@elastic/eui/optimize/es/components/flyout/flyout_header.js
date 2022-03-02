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
export var EuiFlyoutHeader = function EuiFlyoutHeader(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$hasBorder = _ref.hasBorder,
      hasBorder = _ref$hasBorder === void 0 ? false : _ref$hasBorder,
      rest = _objectWithoutProperties(_ref, ["children", "className", "hasBorder"]);

  var classes = classNames('euiFlyoutHeader', {
    'euiFlyoutHeader--hasBorder': hasBorder
  }, className);
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), children);
};