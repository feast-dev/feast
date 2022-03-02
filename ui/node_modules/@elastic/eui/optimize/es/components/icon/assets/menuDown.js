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

var EuiIconMenuDown = function EuiIconMenuDown(_ref) {
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
    d: "M6 7.5c0 .276-.216.5-.495.5h-2.01a.503.503 0 01-.487-.412L3 7.5c0-.276.216-.5.495-.5h2.01c.243 0 .445.183.487.412L6 7.5zM3.51 4a.513.513 0 01-.502-.412L3 3.5c0-.276.228-.5.51-.5h8.98c.25 0 .459.183.502.412L13 3.5c0 .276-.228.5-.51.5H8.493v7.792l2.06-2.06a.5.5 0 11.707.707L9.14 12.56A1.496 1.496 0 018.026 13L7.993 13a.501.501 0 01-.118-.014 1.493 1.493 0 01-.857-.426l-2.122-2.12a.5.5 0 01.708-.708l1.889 1.89V4H3.51zM13 7.5c0 .276-.216.5-.495.5h-2.01a.503.503 0 01-.487-.412L10 7.5c0-.276.216-.5.495-.5h2.01c.243 0 .445.183.487.412L13 7.5z"
  }));
};

export var icon = EuiIconMenuDown;