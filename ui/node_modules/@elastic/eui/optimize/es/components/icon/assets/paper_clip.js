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

var EuiIconPaperClip = function EuiIconPaperClip(_ref) {
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
    d: "M9.84 2.019L3.046 8.57c-.987.952-1.133 2.517-.199 3.516.951 1.021 2.58 1.106 3.64.19.034-.03.068-.061.1-.092l5.655-5.452a.484.484 0 000-.703.53.53 0 00-.729 0L5.92 11.421c-.572.551-1.505.657-2.131.163a1.455 1.455 0 01-.118-2.211l6.899-6.651a2.646 2.646 0 013.644 0 2.422 2.422 0 010 3.513L7.3 12.901c-1.333 1.285-3.497 1.493-4.95.336-1.54-1.22-1.764-3.411-.5-4.897a3.33 3.33 0 01.238-.252l5.78-5.572a.484.484 0 000-.703.53.53 0 00-.73 0l-5.78 5.572a4.36 4.36 0 000 6.324c2.188 2.109 5.202 1.31 6.66-.095l6.925-6.676a3.39 3.39 0 000-4.92C13.534.66 11.25.66 9.841 2.019z"
  }));
};

export var icon = EuiIconPaperClip;