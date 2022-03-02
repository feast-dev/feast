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

var EuiIconNumber = function EuiIconNumber(_ref) {
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
    d: "M7.808 10.197H6.796L5.859 13H4.485l.937-2.803H3.966l.219-1.25h1.647l.608-1.805H4.991l.226-1.251h1.64l.95-2.844h1.368l-.95 2.844h1.018l.95-2.844h1.374l-.95 2.844h1.51l-.218 1.25h-1.702l-.608 1.805h1.497l-.219 1.251H9.182L8.252 13H6.878l.93-2.803zm-.602-1.25h1.012l.615-1.805H7.814l-.608 1.804z",
    fillRule: "evenodd"
  }));
};

export var icon = EuiIconNumber;