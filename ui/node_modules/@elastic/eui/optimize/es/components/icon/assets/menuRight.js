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

var EuiIconMenuRight = function EuiIconMenuRight(_ref) {
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
    d: "M14.986 7.382a.501.501 0 01.013.152c.014.4-.133.806-.439 1.112l-2.12 2.122a.5.5 0 11-.708-.708L13.792 8H1.5a.5.5 0 010-1h12.121l-1.889-1.89a.5.5 0 01.707-.706l2.121 2.12c.241.242.383.544.426.858zM1.5 3h7a.5.5 0 010 1h-7a.5.5 0 010-1zm0 8h7a.5.5 0 110 1h-7a.5.5 0 110-1z"
  }));
};

export var icon = EuiIconMenuRight;