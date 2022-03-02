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

var EuiIconLogoAerospike = function EuiIconLogoAerospike(_ref) {
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
    fill: "#C4373A",
    d: "M0 31.806h32V.776H0z"
  }), ___EmotionJSX("path", {
    fill: "#FFF",
    d: "M19.448 12.508l-8.986 3.86 8.986 3.883v-7.743zm-11.304 4.73l-2.094-.863 2.094-.931 17.749-7.741v2.062l-4.654 1.99v9.25l4.654 2.01v1.968L8.143 17.24z"
  })));
};

export var icon = EuiIconLogoAerospike;