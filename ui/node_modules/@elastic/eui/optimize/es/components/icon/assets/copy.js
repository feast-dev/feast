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

var EuiIconCopy = function EuiIconCopy(_ref) {
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
    d: "M11.4 0c.235 0 .46.099.622.273l2.743 3c.151.162.235.378.235.602v9.25a.867.867 0 01-.857.875H3.857A.867.867 0 013 13.125V.875C3 .392 3.384 0 3.857 0H11.4zM14 4h-2.6a.4.4 0 01-.4-.4V1H4v12h10V4z"
  }), ___EmotionJSX("path", {
    d: "M3 1H2a1 1 0 00-1 1v13a1 1 0 001 1h10a1 1 0 001-1v-1h-1v1H2V2h1V1z"
  }));
};

export var icon = EuiIconCopy;