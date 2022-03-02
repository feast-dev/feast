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

var EuiIconMenuUp = function EuiIconMenuUp(_ref) {
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
    d: "M10.01 8.5c0-.276.216-.5.495-.5h2.01c.243 0 .445.183.487.412l.008.088c0 .276-.216.5-.495.5h-2.01a.503.503 0 01-.487-.412L10.01 8.5zM12.5 12c.25 0 .459.183.502.412l.008.088c0 .276-.228.5-.51.5H3.52a.513.513 0 01-.502-.412L3.01 12.5c0-.276.228-.5.51-.5h3.987V4.208l-2.06 2.06a.5.5 0 11-.707-.707L6.86 3.44A1.496 1.496 0 017.974 3L8.007 3c.04 0 .08.005.118.014.314.043.616.185.857.426l2.122 2.12a.5.5 0 01-.708.708l-1.889-1.89V12H12.5zM3 8.5c0-.276.216-.5.495-.5h2.01c.243 0 .445.183.487.412L6 8.5c0 .276-.216.5-.495.5h-2.01a.503.503 0 01-.487-.412L3 8.5z"
  }));
};

export var icon = EuiIconMenuUp;