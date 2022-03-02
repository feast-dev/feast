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

var EuiIconAppSecurityAnalytics = function EuiIconAppSecurityAnalytics(_ref) {
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
    d: "M3 10h4v2H5v7.928c0 1.299.808 2.795 2.88 4.48 1.83 1.489 4.524 3.02 8.12 4.584V26h2v5.992l-1.38-.567c-4.372-1.797-7.724-3.613-10-5.465C4.358 24.122 3 22.114 3 19.928V10z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M9 10h9v14l-1.272-.458c-1.367-.494-3.23-1.314-4.768-2.39C10.484 20.118 9 18.636 9 16.761V10zm1.895 1.876v4.887c0 .877.744 1.867 2.158 2.856.937.656 2.038 1.219 3.052 1.657v-9.4h-5.21z"
  }), ___EmotionJSX("path", {
    d: "M29 1H9v7h2V2.966h16V16.73c0 .558-.245 1.128-.756 1.72-.515.596-1.256 1.158-2.12 1.668-1.381.818-2.961 1.434-4.124 1.817V24c1.26-.378 3.334-1.12 5.155-2.197.965-.57 1.905-1.261 2.612-2.08.712-.822 1.233-1.827 1.233-2.992V1z"
  }));
};

export var icon = EuiIconAppSecurityAnalytics;