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

var EuiIconLogoOsquery = function EuiIconLogoOsquery(_ref) {
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
    fill: "#A596FF",
    d: "M31.907.032v7.969l-7.952 7.951V7.967L31.907.032"
  }), ___EmotionJSX("path", {
    fill: "#333",
    d: "M16.003.032v7.969l7.952 7.951V7.967L16.003.032"
  }), ___EmotionJSX("path", {
    fill: "#A596FF",
    d: "M31.923 31.855h-7.968l-7.952-7.951h7.985l7.935 7.951"
  }), ___EmotionJSX("path", {
    fill: "#333",
    d: "M31.923 15.952h-7.968l-7.952 7.952h7.985l7.935-7.952"
  }), ___EmotionJSX("path", {
    fill: "#A596FF",
    d: "M.1 31.872v-7.968l7.952-7.952v7.985L.1 31.872"
  }), ___EmotionJSX("path", {
    fill: "#333",
    d: "M16.004 31.872v-7.968l-7.952-7.952v7.985l7.952 7.935"
  }), ___EmotionJSX("path", {
    fill: "#A596FF",
    d: "M.084.048h7.968L16.004 8H8.019L.084.048"
  }), ___EmotionJSX("path", {
    fill: "#333",
    d: "M.084 15.952h7.968L16.004 8H8.019L.084 15.952"
  })));
};

export var icon = EuiIconLogoOsquery;