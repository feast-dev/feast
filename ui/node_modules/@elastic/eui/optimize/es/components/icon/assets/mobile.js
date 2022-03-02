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

var EuiIconMobile = function EuiIconMobile(_ref) {
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
    d: "M6 2.5a.5.5 0 01.5-.5h3a.5.5 0 010 1h-3a.5.5 0 01-.5-.5z"
  }), ___EmotionJSX("path", {
    fillRule: "evenodd",
    clipRule: "evenodd",
    d: "M4.5 0A1.5 1.5 0 003 1.5v13A1.5 1.5 0 004.5 16h7a1.5 1.5 0 001.5-1.5v-13A1.5 1.5 0 0011.5 0h-7zM4 1.5a.5.5 0 01.5-.5h7a.5.5 0 01.5.5V4H4V1.5zM4 13v1.5a.5.5 0 00.5.5h7a.5.5 0 00.5-.5V13H4zm0-1h8V5H4v7z"
  }));
};

export var icon = EuiIconMobile;