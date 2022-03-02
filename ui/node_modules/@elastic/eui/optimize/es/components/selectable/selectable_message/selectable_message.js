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
import { EuiText } from '../../text';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiSelectableMessage = function EuiSelectableMessage(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$bordered = _ref.bordered,
      bordered = _ref$bordered === void 0 ? false : _ref$bordered,
      rest = _objectWithoutProperties(_ref, ["children", "className", "bordered"]);

  var classes = classNames('euiSelectableMessage', {
    'euiSelectableMessage--bordered': bordered
  }, className);
  return ___EmotionJSX(EuiText, _extends({
    color: "subdued",
    size: "xs",
    className: classes
  }, rest), children);
};