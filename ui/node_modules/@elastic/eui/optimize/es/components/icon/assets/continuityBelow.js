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

var EuiIconContinuityBelow = function EuiIconContinuityBelow(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    xmlns: "http://www.w3.org/2000/svg",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M11.5 3a.5.5 0 01.5.5v9a.5.5 0 01-1 0V9H5v1.5a.5.5 0 01-.82.384l-3-2.5a.5.5 0 010-.768l3-2.5A.5.5 0 015 5.5V7h6V3.5a.5.5 0 01.5-.5zm3 0a.5.5 0 01.5.5v9a.5.5 0 01-1 0v-9a.5.5 0 01.5-.5z"
  }));
};

export var icon = EuiIconContinuityBelow;