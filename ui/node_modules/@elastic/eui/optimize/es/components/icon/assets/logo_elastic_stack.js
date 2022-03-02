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

var EuiIconLogoElasticStack = function EuiIconLogoElasticStack(_ref) {
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
    fill: "none",
    fillRule: "evenodd"
  }, ___EmotionJSX("path", {
    fill: "#F04E98",
    d: "M32 9V2.5A2.5 2.5 0 0029.5 0h-27A2.5 2.5 0 000 2.5V9h32z"
  }), ___EmotionJSX("path", {
    fill: "#00BFB3",
    d: "M0 20h32v-8H0z"
  }), ___EmotionJSX("path", {
    fill: "#0080D5",
    d: "M14.5 23H0v6.5A2.5 2.5 0 002.5 32h12v-9z"
  }), ___EmotionJSX("path", {
    fill: "#FEC514",
    d: "M17.5 23v9h12a2.5 2.5 0 002.5-2.5V23H17.5z"
  })));
};

export var icon = EuiIconLogoElasticStack;