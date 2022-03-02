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

var EuiIconVisPie = function EuiIconVisPie(_ref) {
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
    d: "M6.5 9a.5.5 0 01-.5-.5V3.023A5.5 5.5 0 1011.978 9H6.5zM7 8h5.5a.5.5 0 01.5.5A6.5 6.5 0 116.5 2a.5.5 0 01.5.5V8zm2-6.972V6h4.972C13.696 3.552 11.448 1.304 9 1.028zM14.5 7h-6a.5.5 0 01-.5-.5v-6a.5.5 0 01.5-.5C11.853 0 15 3.147 15 6.5a.5.5 0 01-.5.5zM6.146 8.854a.5.5 0 11.708-.708l4 4a.5.5 0 01-.708.708l-4-4z"
  }));
};

export var icon = EuiIconVisPie;