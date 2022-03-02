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

var EuiIconTokenFunction = function EuiIconTokenFunction(_ref) {
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
    d: "M11.266 4.15V5.63a2.41 2.41 0 00-.859-.145c-.692 0-1.065.346-1.177 1.043l-.11.625h1.828v1.44H8.921l-.204 1.115C8.455 11.325 7.517 12 5.9 12c-.469 0-.882-.061-1.166-.167v-1.495c.273.117.591.178.903.178.659 0 1.01-.29 1.127-1.015l.157-.91H5.247V7.152h1.837l.188-.842C7.534 4.714 8.432 4 10.19 4c.39 0 .853.067 1.076.15z"
  }));
};

export var icon = EuiIconTokenFunction;