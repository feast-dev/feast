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

var EuiIconMlRegressionJob = function EuiIconMlRegressionJob(_ref) {
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
    d: "M24 0a8 8 0 11-4.906 14.32l-4.774 4.774a8 8 0 11-1.414-1.414l4.774-4.774A8 8 0 0124 0zM8 18a6 6 0 100 12 6 6 0 000-12zM24 2a6 6 0 100 12 6 6 0 000-12z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M32 20v12H20V20h12zm-2 2h-8v8h8v-8zM12 0v12H0V0h12zm-2 2H2v8h8V2z"
  }));
};

export var icon = EuiIconMlRegressionJob;