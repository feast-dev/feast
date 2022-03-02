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

var EuiIconSun = function EuiIconSun(_ref) {
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
    d: "M12 8a4 4 0 11-8 0 4 4 0 018 0zM7.25.75a.75.75 0 011.5 0v1.5a.75.75 0 11-1.5 0V.75zm0 13a.75.75 0 011.5 0v1.5a.75.75 0 01-1.5 0v-1.5zm5.346-11.407a.75.75 0 011.06 1.06l-1.06 1.061a.75.75 0 01-1.06-1.06l1.06-1.06zm-9.192 9.193a.75.75 0 111.06 1.06l-1.06 1.06a.75.75 0 01-1.06-1.06l1.06-1.06zM.75 8.75a.75.75 0 010-1.5h1.5a.75.75 0 110 1.5H.75zm13 0a.75.75 0 010-1.5h1.5a.75.75 0 010 1.5h-1.5zM2.343 3.404a.75.75 0 111.06-1.06l1.061 1.06a.75.75 0 01-1.06 1.06l-1.06-1.06zm9.193 9.192a.75.75 0 011.06-1.06l1.06 1.06a.75.75 0 01-1.06 1.06l-1.06-1.06z"
  }));
};

export var icon = EuiIconSun;