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

var EuiIconTokenSymbol = function EuiIconTokenSymbol(_ref) {
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
    d: "M8.316 14a6 6 0 110-12 6 6 0 010 12zm0-1.333a4.667 4.667 0 100-9.334 4.667 4.667 0 000 9.334zm2.19-5.72h1.143c.019 1.448-.793 2.338-1.922 2.338-.632 0-1.194-.267-1.706-.811-.36-.397-.636-.576-1-.576-.517 0-.849.355-.885 1.083H4.983c.014-1.47.858-2.314 1.95-2.314.595 0 1.125.249 1.678.802.392.382.641.595 1.038.595.484 0 .857-.323.857-1.116z"
  }));
};

export var icon = EuiIconTokenSymbol;