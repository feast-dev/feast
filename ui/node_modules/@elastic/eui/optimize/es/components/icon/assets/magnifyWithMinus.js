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

var EuiIconMagnifyWithMinus = function EuiIconMagnifyWithMinus(_ref) {
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
    d: "M9.5 6a.5.5 0 010 1h-6a.5.5 0 010-1h6zm.74 4.74c0-.117.04-.225.107-.31A5.478 5.478 0 0012 6.5 5.5 5.5 0 106.5 12a.5.5 0 110 1 6.5 6.5 0 114.936-2.27l4.419 4.418a.5.5 0 01-.707.707l-4.768-4.768a.499.499 0 01-.14-.347z"
  }));
};

export var icon = EuiIconMagnifyWithMinus;