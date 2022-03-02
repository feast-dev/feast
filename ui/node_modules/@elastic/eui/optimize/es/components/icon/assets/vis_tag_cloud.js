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

var EuiIconVisTagCloud = function EuiIconVisTagCloud(_ref) {
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
    d: "M1.5 9.047a.5.5 0 100 1h13a.5.5 0 000-1h-13zm0-1h13a1.5 1.5 0 010 3h-13a1.5 1.5 0 010-3zM10 13a.5.5 0 110 1H4a.5.5 0 110-1h6zM8.001 2.015a.5.5 0 11-.002 1l-5-.015a.5.5 0 11.003-1l5 .015zM14 5a.5.5 0 110 1H6a.5.5 0 010-1h8z"
  }));
};

export var icon = EuiIconVisTagCloud;