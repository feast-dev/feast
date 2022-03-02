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

var EuiIconAppTimelion = function EuiIconAppTimelion(_ref) {
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
    d: "M4 4v20.34L16 32l12-7.64V4H4zm22 2v4h-4a3 3 0 00-3 3v5h-6v-5a3 3 0 00-3-3H6V6h20zm-7.87 14L16 22.52 13.87 20h4.26zM6 12h4a1 1 0 011 1v6.7l3.69 4.37-2.58 3.06L6 23.24V12zm7.81 16.22l2.19-2.6 2.19 2.6L16 29.61l-2.19-1.39zm6.08-1.09l-2.58-3.06L21 19.7V13a1 1 0 011-1h4v11.24l-6.11 3.89z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M4 0h24v2H4z"
  }));
};

export var icon = EuiIconAppTimelion;