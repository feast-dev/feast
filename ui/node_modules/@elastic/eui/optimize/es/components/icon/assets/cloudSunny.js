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

var EuiIconCloudSunny = function EuiIconCloudSunny(_ref) {
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
    d: "M10.746 5.005A5.5 5.5 0 0110.5 16H4a4 4 0 01-1.61-7.663A4.473 4.473 0 012.029 7H.5a.5.5 0 010-1h1.527a4.479 4.479 0 01.957-2.309L1.646 2.354a.5.5 0 11.708-.708L3.69 2.984A4.479 4.479 0 016 2.027V.5a.5.5 0 011 0v1.528a4.493 4.493 0 012.309.956l1.337-1.338a.5.5 0 01.708.708L10.016 3.69c.311.388.56.831.73 1.314zM4 15h6.5a4.5 4.5 0 10-4.152-6.239A3.995 3.995 0 018 12a.5.5 0 11-1 0 3 3 0 10-3 3zm5.691-9.94a3.5 3.5 0 10-6.33 2.991 4.029 4.029 0 012.106.227 5.505 5.505 0 014.224-3.219z"
  }));
};

export var icon = EuiIconCloudSunny;