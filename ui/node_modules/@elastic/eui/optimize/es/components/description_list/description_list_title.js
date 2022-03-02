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
export var EuiDescriptionListTitle = function EuiDescriptionListTitle(_ref) {
  var children = _ref.children,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["children", "className"]);

  var classes = classNames('euiDescriptionList__title', className);
  return ___EmotionJSX("dt", _extends({
    className: classes
  }, rest), children);
};