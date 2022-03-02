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

var EuiIconAppSecurity = function EuiIconAppSecurity(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M14 32l-.36-.14A21.07 21.07 0 010 12.07V5.44L14 .06l14 5.38v6.63a21.07 21.07 0 01-13.64 19.78L14 32zM2 6.82v5.25a19.08 19.08 0 0012 17.77 19.08 19.08 0 0012-17.77V6.82L14 2.2 2 6.82z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M9 17.83h2V23H9zM11 10.18V7H9v3.18a3 3 0 102 0zM10 14a1 1 0 110-2 1 1 0 010 2zM17 7h2v5.17h-2zM21 17a3 3 0 10-4 2.82V23h2v-3.18A3 3 0 0021 17zm-3 1a1 1 0 110-2 1 1 0 010 2z"
  }));
};

export var icon = EuiIconAppSecurity;