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
import { keysOf } from '../common';
import { EuiIcon } from '../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  m: 'euiLoadingLogo--medium',
  l: 'euiLoadingLogo--large',
  xl: 'euiLoadingLogo--xLarge'
};
export var SIZES = keysOf(sizeToClassNameMap);
export var EuiLoadingLogo = function EuiLoadingLogo(_ref) {
  var _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      _ref$logo = _ref.logo,
      logo = _ref$logo === void 0 ? 'logoKibana' : _ref$logo,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["size", "logo", "className"]);

  var classes = classNames('euiLoadingLogo', sizeToClassNameMap[size], className);
  return ___EmotionJSX("span", _extends({
    className: classes
  }, rest), ___EmotionJSX("span", {
    className: "euiLoadingLogo__icon"
  }, ___EmotionJSX(EuiIcon, {
    type: logo,
    size: size
  })));
};