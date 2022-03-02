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

var EuiIconTimeslider = function EuiIconTimeslider(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 16,
    height: 16,
    fill: "none",
    viewBox: "0 0 16 16",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M13.923 8A5.93 5.93 0 008 2.077 5.93 5.93 0 002.077 8a5.93 5.93 0 006.296 5.912c.328-.021.625.227.625.556a.504.504 0 01-.458.511 7 7 0 116.43-6.333c-.036.398-.487.58-.817.354a.595.595 0 01-.248-.54c.012-.152.018-.305.018-.46zm1.684 3.2l-4.32-3.055c-.56-.396-1.287.059-1.287.8v6.108c0 .74.726 1.196 1.287.8l4.32-3.055c.524-.37.524-1.228 0-1.598zM7.462 7.462H4.769a.539.539 0 000 1.076H8A.539.539 0 008.538 8V3.692a.539.539 0 00-1.076 0v3.77z",
    clipRule: "evenodd"
  }));
};

export var icon = EuiIconTimeslider;