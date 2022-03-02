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

var EuiIconAppDiscover = function EuiIconAppDiscover(_ref) {
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
    d: "M8.33 23.67l4.79-10.55 10.55-4.79-4.79 10.55-10.55 4.79zm6.3-9l-2.28 5 5-2.28 2.28-5-5 2.28z"
  }), ___EmotionJSX("path", {
    d: "M16 0C7.163 0 0 7.163 0 16s7.163 16 16 16 16-7.163 16-16A16 16 0 0016 0zm1 29.95V28h-2v1.95A14 14 0 012.05 17H4v-2H2.05A14 14 0 0115 2.05V4h2V2.05A14 14 0 0129.95 15H28v2h1.95A14 14 0 0117 29.95z"
  }));
};

export var icon = EuiIconAppDiscover;