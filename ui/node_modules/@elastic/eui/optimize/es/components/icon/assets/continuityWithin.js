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

var EuiIconContinuityWithin = function EuiIconContinuityWithin(_ref) {
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
    d: "M.5 3a.5.5 0 00-.5.5v9a.5.5 0 001 0v-9A.5.5 0 00.5 3zm14.5.5a.5.5 0 011 0v9a.5.5 0 01-1 0v-9zm-4.712 1.547a.5.5 0 01.532.069l3 2.5a.5.5 0 010 .768l-3 2.5A.5.5 0 0110 10.5V9H6v1.5a.5.5 0 01-.82.384l-3-2.5a.5.5 0 010-.768l3-2.5A.5.5 0 016 5.5V7h4V5.5a.5.5 0 01.288-.453z"
  }));
};

export var icon = EuiIconContinuityWithin;