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

var EuiIconMlCreateAdvancedJob = function EuiIconMlCreateAdvancedJob(_ref) {
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
    d: "M16 32C7.163 32 0 24.837 0 16S7.163 0 16 0s16 7.163 16 16h-2c0-7.732-6.268-14-14-14S2 8.268 2 16s6.268 14 14 14v2z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M27 20v12h-2V20h2zm-5 4v8h-2v-8h2zm10-2v10h-2V22h2zM17 9v6h6v2h-6v6h-2v-6H9v-2h6V9h2z"
  }));
};

export var icon = EuiIconMlCreateAdvancedJob;