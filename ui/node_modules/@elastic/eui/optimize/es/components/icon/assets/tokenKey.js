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

var EuiIconTokenKey = function EuiIconTokenKey(_ref) {
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
    fillRule: "evenodd",
    d: "M12.667 6.542A3.208 3.208 0 018.86 9.694l-.438.492a.437.437 0 01-.327.147h-.678v.73a.437.437 0 01-.438.437H6.25v.73a.437.437 0 01-.438.437H3.772a.437.437 0 01-.438-.438v-1.423c0-.116.046-.227.128-.31l2.95-2.949a3.208 3.208 0 013.047-4.214 3.202 3.202 0 013.209 3.209zm-3.209-.875a.875.875 0 101.75 0 .875.875 0 00-1.75 0z"
  }));
};

export var icon = EuiIconTokenKey;