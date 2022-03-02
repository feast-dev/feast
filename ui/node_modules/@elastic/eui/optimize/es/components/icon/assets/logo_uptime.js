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

var EuiIconLogoUptime = function EuiIconLogoUptime(_ref) {
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
    fill: "#3EBEB0",
    d: "M19 15c0 7.062-4.888 12.969-11.46 14.563A15.914 15.914 0 0016 32c8.836 0 16-7.163 16-16v-3l-6.5-6-6.5 6v2z"
  }), ___EmotionJSX("path", {
    fill: "#07C",
    d: "M6.833 26.646a11.954 11.954 0 008.544-7.834A12.43 12.43 0 0113 11.5V.292C5.6 1.696 0 8.19 0 16.002c0 4.358 1.75 8.306 4.577 11.192l2.256-.547z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillNegative",
    d: "M30.362 23.02c-1.494.63-3.137.98-4.861.98a12.443 12.443 0 01-7.852-2.78 15.042 15.042 0 01-10.11 8.343A15.91 15.91 0 0016.002 32c6.314 0 11.758-3.669 14.36-8.98"
  }));
};

export var icon = EuiIconLogoUptime;