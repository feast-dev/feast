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

var EuiIconLogoSketch = function EuiIconLogoSketch(_ref) {
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
  }, title) : null, ___EmotionJSX("g", {
    fill: "none"
  }, ___EmotionJSX("path", {
    fill: "#FFAE00",
    d: "M16 32L0 11.454l6.931-10.38L16 0l9.069 1.074L32 11.454z"
  }), ___EmotionJSX("path", {
    fill: "#EC6C00",
    d: "M16 32L0 11.454h32z"
  }), ___EmotionJSX("path", {
    fill: "#FFAE00",
    d: "M16 32L6.477 11.454h19.045z"
  }), ___EmotionJSX("path", {
    fill: "#FFEFB4",
    d: "M16 0L6.477 11.454h19.045z"
  }), ___EmotionJSX("path", {
    fill: "#FFAE00",
    d: "M6.932 1.074L3.369 6.3.001 11.454h6.542zM25.069 1.074L28.632 6.3 32 11.454h-6.542z"
  }), ___EmotionJSX("path", {
    fill: "#FED305",
    d: "M6.931 1.074l-.453 10.38L16 0zM25.069 1.074l.453 10.38L16 0z"
  })));
};

export var icon = EuiIconLogoSketch;