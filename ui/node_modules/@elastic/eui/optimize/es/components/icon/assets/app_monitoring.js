import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// THIS IS A GENERATED FILE. DO NOT MODIFY MANUALLY. @see scripts/compile-icons.js
import * as React from 'react';
import { jsx as ___EmotionJSX } from "@emotion/react";

var EuiIconAppMonitoring = function EuiIconAppMonitoring(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M1.81 15.19A8.94 8.94 0 0115.62 3.86l.38.42.38-.42a8.94 8.94 0 0114.26 10.68l-1.7-1.07a6.94 6.94 0 00-11.07-8.28L16 7.29l-1.87-2.1A6.94 6.94 0 003.41 14l-1.6 1.19zM16 31.18l-7.74-8.51 1.48-1.34L16 28.21l6.26-6.88 1.48 1.34z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M16.16 23.29l-4.1-7.17L10.62 19H0v-2h9.38l2.56-5.12 3.9 6.83 4.13-10.32L23.66 17H32v2h-9.66l-2.31-5.39z"
  }));
};

export var icon = EuiIconAppMonitoring;