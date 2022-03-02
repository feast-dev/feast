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

var EuiIconCompute = function EuiIconCompute(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M4 3a1 1 0 00-1 1v8a1 1 0 001 1h8a1 1 0 001-1V4a1 1 0 00-1-1H4zm10 8v1a2 2 0 01-2 2h-1v2h-1v-2H8.5v2h-1v-2H6v2H5v-2H4a2 2 0 01-2-2v-1H0v-1h2V8.5H0v-1h2V6H0V5h2V4a2 2 0 012-2h1V0h1v2h1.5V0h1v2H10V0h1v2h1a2 2 0 012 2v1h2v1h-2v1.5h2v1h-2V10h2v1h-2z"
  }), ___EmotionJSX("rect", {
    width: 6,
    height: 6,
    x: 5,
    y: 5,
    rx: 1
  }));
};

export var icon = EuiIconCompute;