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

var EuiIconAppAgent = function EuiIconAppAgent(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    xmlns: "http://www.w3.org/2000/svg",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M21 2.82L16 .038 11 2.82v2.289l5-2.782 5 2.782v-2.29z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M21 7.282L16 4.5l-5 2.782V9.57l5-2.781 5 2.781V7.282z"
  }), ___EmotionJSX("path", {
    d: "M7 5.045L2 7.827v15.577l14 7.788 14-7.788V7.827l-5-2.782v2.289l3 1.669v13.225l-12 6.676-12-6.676V9.003l3-1.669V5.045z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    fillRule: "evenodd",
    clipRule: "evenodd",
    d: "M22 12.5L16 9l-6 3.5v7l6 3.5 6-3.5v-7zm-9.974 1.205L16 11.387l3.974 2.318v4.59L16 20.613l-3.974-2.318v-4.59z"
  }));
};

export var icon = EuiIconAppAgent;