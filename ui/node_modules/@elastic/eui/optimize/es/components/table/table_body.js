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
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiTableBody = function EuiTableBody(_ref) {
  var children = _ref.children,
      className = _ref.className,
      bodyRef = _ref.bodyRef,
      rest = _objectWithoutProperties(_ref, ["children", "className", "bodyRef"]);

  return ___EmotionJSX("tbody", _extends({
    className: className,
    ref: bodyRef
  }, rest), children);
};