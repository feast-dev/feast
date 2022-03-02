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

var EuiIconHeatmap = function EuiIconHeatmap(_ref) {
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
    d: "M12 9a3 3 0 110 6 3 3 0 010-6zm0 1a2 2 0 100 4 2 2 0 000-4zM4 2a2 2 0 110 4 2 2 0 010-4zm0 1a1 1 0 100 2 1 1 0 000-2zm0 5a4 4 0 110 8 4 4 0 010-8zm0 1a3 3 0 100 6 3 3 0 000-6zm8-9a4 4 0 110 8 4 4 0 010-8zm0 1a3 3 0 100 6 3 3 0 000-6z"
  }));
};

export var icon = EuiIconHeatmap;