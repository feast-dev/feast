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

var EuiIconLogoLogging = function EuiIconLogoLogging(_ref) {
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
    fill: "#0080D5",
    d: "M2 10v13a9 9 0 019 9h13c0-12.15-9.85-22-22-22"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillNegative",
    d: "M14 13.565V32h10c0-7.722-3.981-14.51-10-18.436"
  }), ___EmotionJSX("path", {
    fill: "#00BFB3",
    d: "M14 0v10.226C21.666 14.468 26.869 22.636 26.869 32H30V16c0-8.837-7.163-16-16-16"
  }));
};

export var icon = EuiIconLogoLogging;