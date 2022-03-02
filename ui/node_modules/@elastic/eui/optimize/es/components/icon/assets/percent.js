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

var EuiIconPercent = function EuiIconPercent(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    xmlns: "http://www.w3.org/2000/svg",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    fillRule: "evenodd",
    d: "M5 8c1.105 0 2-1.12 2-2.5S6.105 3 5 3 3 4.12 3 5.5 3.895 8 5 8zm0-1c.356 0 1-.452 1-1.5S5.356 4 5 4s-1 .452-1 1.5S4.644 7 5 7z"
  }), ___EmotionJSX("path", {
    d: "M10.5 3H12L5.5 13H4l6.5-10z"
  }), ___EmotionJSX("path", {
    fillRule: "evenodd",
    d: "M13 10.5c0 1.38-.895 2.5-2 2.5s-2-1.12-2-2.5S9.895 8 11 8s2 1.12 2 2.5zm-1 0c0 1.048-.644 1.5-1 1.5s-1-.452-1-1.5.644-1.5 1-1.5 1 .452 1 1.5z"
  }));
};

export var icon = EuiIconPercent;