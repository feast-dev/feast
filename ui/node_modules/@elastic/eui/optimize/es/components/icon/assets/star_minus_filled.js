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

var EuiIconStarMinusFilled = function EuiIconStarMinusFilled(_ref) {
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
    d: "M6 2a.86.86 0 01.792.511l1.33 2.924 3.128.446c.71.102 1.001.976.496 1.487L9.433 9.704l.563 3.268A.877.877 0 019.136 14a.862.862 0 01-.429-.116L6 12.342l-2.707 1.542a.862.862 0 01-.43.116.877.877 0 01-.859-1.027l.563-3.269L.254 7.368C-.25 6.857.04 5.983.75 5.88l3.128-.446 1.33-2.923A.86.86 0 016 2zm10 8v1h-5v-1h5z"
  }));
};

export var icon = EuiIconStarMinusFilled;