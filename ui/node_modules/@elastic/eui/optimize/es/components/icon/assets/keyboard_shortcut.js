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

var EuiIconKeyboardShortcut = function EuiIconKeyboardShortcut(_ref) {
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
    d: "M15 9a1 1 0 01.993.883L16 10v5a1 1 0 01-.883.993L15 16H7a1 1 0 01-.993-.883L6 15v-5a1 1 0 01.883-.993L7 9h8zM2.5 10a.5.5 0 01.492.41L3 10.5V12h1.5a.5.5 0 01.09.992L4.5 13H3v1.5a.5.5 0 01-.992.09L2 14.5V13H.5a.5.5 0 01-.09-.992L.5 12H2v-1.5a.5.5 0 01.5-.5zM15 10H7v5h8v-5zm-1 3v1H8v-1h6zm1-13a1 1 0 011 1v5a1 1 0 01-1 1H1a1 1 0 01-1-1V1a1 1 0 011-1h14zm0 1H1v5h14V1zM8 4v1H2V4h6z"
  }));
};

export var icon = EuiIconKeyboardShortcut;