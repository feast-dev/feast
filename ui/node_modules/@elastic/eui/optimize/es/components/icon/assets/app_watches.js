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

var EuiIconAppWatches = function EuiIconAppWatches(_ref) {
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
    d: "M9.74 7.73l-1.5-1.32a13 13 0 000 17.19l1.5-1.32a11 11 0 010-14.54v-.01z"
  }), ___EmotionJSX("path", {
    d: "M6.51 3.66L5 2.34c-6.377 7.24-6.377 18.09 0 25.33l1.5-1.32C.792 19.867.792 10.153 6.5 3.67l.01-.01zm17.25 2.75l-1.5 1.32a11 11 0 010 14.54l1.5 1.32a13 13 0 000-17.19v.01z"
  }), ___EmotionJSX("path", {
    d: "M27 2.34l-1.5 1.32c5.708 6.483 5.708 16.197 0 22.68l1.5 1.33c6.377-7.24 6.377-18.09 0-25.33z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M21 15a5 5 0 10-6 4.9V31h2V19.9a5 5 0 004-4.9zm-5 3a3 3 0 110-6 3 3 0 010 6z"
  }));
};

export var icon = EuiIconAppWatches;