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
export var EuiRangeLabel = function EuiRangeLabel(_ref) {
  var children = _ref.children,
      disabled = _ref.disabled,
      _ref$side = _ref.side,
      side = _ref$side === void 0 ? 'max' : _ref$side;
  var classes = classNames('euiRangeLabel', "euiRangeLabel--".concat(side), {
    'euiRangeLabel--isDisabled': disabled
  });
  return ___EmotionJSX("label", {
    className: classes
  }, children);
};