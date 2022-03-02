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

var EuiIconIInCircle = function EuiIconIInCircle(_ref) {
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
    d: "M7.5 11.508L7.468 8H6.25V7h2.401l.03 3.508H9.8v1H7.5zm-.25-6.202a.83.83 0 01.207-.577c.137-.153.334-.229.59-.229.256 0 .454.076.594.23.14.152.209.345.209.576 0 .228-.07.417-.21.568-.14.15-.337.226-.593.226-.256 0-.453-.075-.59-.226a.81.81 0 01-.207-.568zM8 13A5 5 0 108 3a5 5 0 000 10zm0 1A6 6 0 118 2a6 6 0 010 12z"
  }));
};

export var icon = EuiIconIInCircle;