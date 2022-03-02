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

var EuiIconGear = function EuiIconGear(_ref) {
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
    d: "M.164 10.329L1.87 8 .163 5.67c.18-.601.43-1.19.758-1.757a8.197 8.197 0 011.142-1.535l2.872.313L6.099.05a8.166 8.166 0 013.8-.003l1.166 2.644 2.872-.313a8.166 8.166 0 011.899 3.293L14.13 8l1.706 2.33c-.18.601-.43 1.19-.758 1.757a8.197 8.197 0 01-1.142 1.535l-2.872-.313-1.164 2.641a8.166 8.166 0 01-3.8.003l-1.166-2.644-2.872.313a8.166 8.166 0 01-1.899-3.293zm4.663 1.986a1 1 0 011.023.591l.957 2.17c.79.134 1.597.132 2.387-.001l.956-2.169a1 1 0 011.023-.59l2.358.256a7.23 7.23 0 001.194-2.068l-1.401-1.913a1 1 0 010-1.182l1.4-1.912a7.165 7.165 0 00-1.192-2.069l-2.359.257a1 1 0 01-1.023-.591L9.193.924a7.165 7.165 0 00-2.387.001L5.85 3.094a1 1 0 01-1.023.59l-2.358-.256a7.23 7.23 0 00-1.194 2.068l1.401 1.913a1 1 0 010 1.182l-1.4 1.912c.28.751.681 1.45 1.192 2.069l2.359-.257zM8 11a3 3 0 110-6 3 3 0 010 6zm0-1a2 2 0 100-4 2 2 0 000 4z"
  }));
};

export var icon = EuiIconGear;