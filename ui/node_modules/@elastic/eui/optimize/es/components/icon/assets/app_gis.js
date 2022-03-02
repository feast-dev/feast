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

var EuiIconAppGis = function EuiIconAppGis(_ref) {
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
    className: "euiIcon__fillSecondary",
    d: "M29.014 23.89l2.296 1.145L16 32.101.53 24.961l2.49-1.056 2.453 1.132-.003.002L16 29.899l10.69-4.934-.003-.001 2.327-1.074zm-4.972-7.482l7.268 3.627L16 27.101.53 19.961l7.668-3.252c.392.486.838 1.02 1.34 1.604L5.47 20.039 16 24.899l10.69-4.934-3.954-1.973c.493-.58.928-1.107 1.306-1.584z"
  }), ___EmotionJSX("path", {
    d: "M18 9a2 2 0 10-4 0 2 2 0 004 0zm2 0a4 4 0 11-8 0 4 4 0 018 0zm-3.268 12.681l-.732.787-.732-.787c-3.557-3.824-5.817-6.462-6.81-7.96A8.746 8.746 0 017 8.875C7 3.97 11.033 0 16 0s9 3.97 9 8.875a8.746 8.746 0 01-1.459 4.846c-.992 1.498-3.252 4.136-6.809 7.96zm5.142-9.064A6.747 6.747 0 0023 8.875C23 5.081 19.87 2 16 2S9 5.081 9 8.875c0 1.349.394 2.636 1.126 3.742.846 1.277 2.812 3.593 5.874 6.912 3.062-3.32 5.028-5.635 5.874-6.912z"
  }));
};

export var icon = EuiIconAppGis;