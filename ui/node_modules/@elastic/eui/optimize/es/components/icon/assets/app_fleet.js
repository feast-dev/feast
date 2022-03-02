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

var EuiIconAppFleet = function EuiIconAppFleet(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    xmlns: "http://www.w3.org/2000/svg",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M2.996 18.636L0 20.434v6.132l6 3.6 5-3 5 3 5-3 5 3 6-3.6v-6.132l-5-3v-5l-5-3v-5l-6-3.6-6 3.6v2.832l2 .033V5.566l4-2.4 4 2.4v3.868l-1.996 1.197 1 1.733L21 11.166l4 2.4v3.868l-4 2.4-2.257-1.354-.971 1.75L20 21.565v3.868l-4 2.4-4-2.4v-1.7l-2-.033v1.733l-4 2.4-4-2.4v-3.868l1.996-1.197-1-1.733zM22 25.434v-3.868l4-2.4 4 2.4v3.868l-4 2.4-4-2.4z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M11 22.166l-6-3.6v-6.132l6-3.6 6 3.6v6.132l-6 3.6zm4-4.732v-3.868l-4-2.4-4 2.4v3.868l4 2.4 4-2.4z"
  }));
};

export var icon = EuiIconAppFleet;