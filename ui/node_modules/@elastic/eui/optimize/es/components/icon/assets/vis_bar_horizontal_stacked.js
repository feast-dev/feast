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

var EuiIconVisBarHorizontalStacked = function EuiIconVisBarHorizontalStacked(_ref) {
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
    d: "M.5 0a.5.5 0 01.5.5v14a.5.5 0 11-1 0V.5A.5.5 0 01.5 0zm13 1a.5.5 0 01.5.5v4a.5.5 0 01-.5.5H9v3h2.5a.5.5 0 01.5.5v4a.5.5 0 01-.5.5h-9a.5.5 0 110-1H9v-3H2.5a.5.5 0 010-1H6V6H2.5a.5.5 0 010-1H10V2H2.5a.5.5 0 010-1h11z"
  }));
};

export var icon = EuiIconVisBarHorizontalStacked;