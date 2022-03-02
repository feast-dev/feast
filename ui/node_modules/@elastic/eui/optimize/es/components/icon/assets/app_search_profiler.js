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

var EuiIconAppSearchProfiler = function EuiIconAppSearchProfiler(_ref) {
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
    className: "euiIcon__fillSecondary",
    d: "M11.63 8h7.38v2h-7.38z"
  }), ___EmotionJSX("path", {
    d: "M7 8h3.19v2H7z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M7 16h7.38v2H7z"
  }), ___EmotionJSX("path", {
    d: "M15.81 16H19v2h-3.19zM7 12h9v2H7z"
  }), ___EmotionJSX("path", {
    d: "M13 0C5.82 0 0 5.82 0 13s5.82 13 13 13 13-5.82 13-13A13 13 0 0013 0zm0 24C6.925 24 2 19.075 2 13S6.925 2 13 2s11 4.925 11 11-4.925 11-11 11zM22.581 23.993l1.414-1.414 7.708 7.708-1.414 1.414z"
  }));
};

export var icon = EuiIconAppSearchProfiler;