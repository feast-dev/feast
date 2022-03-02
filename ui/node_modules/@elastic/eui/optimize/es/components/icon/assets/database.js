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

var EuiIconDatabase = function EuiIconDatabase(_ref) {
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
    d: "M2 12h12v-1.97c-1.225.582-3.454.97-6 .97s-4.775-.388-6-.97V12zm-1 0V3c0-1.105 3.134-2 7-2s7 .895 7 2v9c0 1.105-3.134 2-7 2s-7-.895-7-2zm1-3h12V7.03c-1.225.582-3.454.97-6 .97s-4.775-.388-6-.97V9zm0-4.97V6h12V4.03c-1.225.582-3.454.97-6 .97s-4.775-.388-6-.97zm10.675-1.483C11.467 2.202 9.795 2 8 2c-1.794 0-3.467.202-4.675.547-.492.14-.88.298-1.136.453.256.155.644.312 1.136.453C4.533 3.798 6.205 4 8 4c1.794 0 3.467-.202 4.675-.547.492-.14.88-.298 1.136-.453-.256-.155-.644-.312-1.136-.453zM2 6c.257.155.833.312 1.325.453C4.533 6.798 6.205 7 8 7c1.794 0 3.467-.202 4.675-.547.492-.14 1.07-.298 1.327-.453H2zm0 3c.257.155.833.312 1.325.453C4.533 9.798 6.205 10 8 10c1.794 0 3.467-.202 4.675-.547.492-.14 1.07-.298 1.327-.453H2zm0 3c.257.155.833.312 1.325.453C4.533 12.798 6.205 13 8 13c1.794 0 3.467-.202 4.675-.547.492-.14 1.07-.298 1.327-.453H2z"
  }));
};

export var icon = EuiIconDatabase;