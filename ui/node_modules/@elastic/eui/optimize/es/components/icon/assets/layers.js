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

var EuiIconLayers = function EuiIconLayers(_ref) {
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
    d: "M7.276 1.053a.5.5 0 01.448 0l6 3a.5.5 0 010 .894l-6 3a.5.5 0 01-.448 0l-6-3a.5.5 0 010-.894l6-3zM2.618 4.5L7.5 6.941 12.382 4.5 7.5 2.059 2.618 4.5z"
  }), ___EmotionJSX("path", {
    d: "M1.053 7.276a.5.5 0 01.67-.223L7.5 9.94l5.776-2.888a.5.5 0 11.448.894l-6 3a.5.5 0 01-.448 0l-6-3a.5.5 0 01-.223-.67z"
  }), ___EmotionJSX("path", {
    d: "M1.724 10.053a.5.5 0 10-.448.894l6 3a.5.5 0 00.448 0l6-3a.5.5 0 10-.448-.894L7.5 12.94l-5.776-2.888z"
  }));
};

export var icon = EuiIconLayers;