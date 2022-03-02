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

var EuiIconLogoElasticsearch = function EuiIconLogoElasticsearch(_ref) {
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
    className: "euiIcon__fillNegative",
    d: "M2 16c0 1.384.194 2.72.524 4H22a4 4 0 000-8H2.524A15.984 15.984 0 002 16"
  }), ___EmotionJSX("path", {
    fill: "#FEC514",
    d: "M28.924 7.662A15.381 15.381 0 0030.48 6C27.547 2.346 23.05 0 18 0 11.679 0 6.239 3.678 3.644 9H25.51a5.039 5.039 0 003.413-1.338"
  }), ___EmotionJSX("path", {
    fill: "#00BFB3",
    d: "M25.51 23H3.645C6.24 28.323 11.679 32 18 32c5.05 0 9.547-2.346 12.48-6a15.381 15.381 0 00-1.556-1.662A5.034 5.034 0 0025.51 23"
  })));
};

export var icon = EuiIconLogoElasticsearch;