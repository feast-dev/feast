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

var EuiIconControlsVertical = function EuiIconControlsVertical(_ref) {
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
    fillRule: "evenodd",
    d: "M10 7.95a2.5 2.5 0 010-4.9V1.5a.5.5 0 111 0v1.55a2.5 2.5 0 010 4.9v6.55a.5.5 0 11-1 0V7.95zm-4 .1a2.5 2.5 0 010 4.9v1.55a.5.5 0 11-1 0v-1.55a2.5 2.5 0 010-4.9V1.5a.5.5 0 011 0v6.55zM5.5 12a1.5 1.5 0 100-3 1.5 1.5 0 000 3zm5-8a1.5 1.5 0 100 3 1.5 1.5 0 000-3z"
  }));
};

export var icon = EuiIconControlsVertical;