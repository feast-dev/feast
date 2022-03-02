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

var EuiIconLogoCloud = function EuiIconLogoCloud(_ref) {
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
  }, title) : null, ___EmotionJSX("g", {
    fill: "none",
    fillRule: "evenodd"
  }, ___EmotionJSX("path", {
    className: "euiIcon__fillNegative",
    d: "M12.37 18.352c.032-.01.066-.014.1-.023A6 6 0 0118 10V0C9.163 0 2 7.164 2 16c0 2.793.72 5.417 1.976 7.702a18.947 18.947 0 018.393-5.349"
  }), ___EmotionJSX("path", {
    fill: "#0080D5",
    d: "M18 0A15.959 15.959 0 005.717 5.75a16.006 16.006 0 007.541 5.032c.71.22 1.477.135 2.146-.188A5.94 5.94 0 0118 10a5.94 5.94 0 012.596.594c.669.323 1.436.408 2.146.188a16.01 16.01 0 007.541-5.032A15.959 15.959 0 0018 0"
  }), ___EmotionJSX("path", {
    fill: "#00BFB3",
    d: "M22.742 21.218c-.71-.22-1.478-.135-2.146.188A5.947 5.947 0 0118 22a5.94 5.94 0 01-2.596-.594c-.669-.323-1.436-.408-2.146-.188a16.006 16.006 0 00-7.54 5.032A15.959 15.959 0 0018 32c4.936 0 9.348-2.236 12.283-5.75a16.016 16.016 0 00-7.54-5.032"
  })));
};

export var icon = EuiIconLogoCloud;