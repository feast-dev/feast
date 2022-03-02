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

var EuiIconAppCode = function EuiIconAppCode(_ref) {
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
    d: "M11.276 29l.594 2H0l7.621-14.29.811 2.73L3.333 29h7.943zM28.92 31l-4.987-16.598A16 16 0 008.688 3l1.8 6H8.4L6 1h2.607a18 18 0 0117.241 12.828L31 31h-2.08z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M12.037 14.02L16.492 29h6.827l-2.333-7.849a10 10 0 00-8.949-7.13zM9.35 12h2.05a12 12 0 0111.503 8.581L26 31H15L9.35 12z"
  }));
};

export var icon = EuiIconAppCode;