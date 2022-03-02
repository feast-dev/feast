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

var EuiIconAppMetricbeat = function EuiIconAppMetricbeat(_ref) {
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
    d: "M16 32C7.163 32 0 24.837 0 16S7.163 0 16 0s16 7.163 16 16a16 16 0 01-16 16zm0-30C8.268 2 2 8.268 2 16s6.268 14 14 14 14-6.268 14-14A14 14 0 0016 2z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M28 16h-2c0-5.523-4.477-10-10-10S6 10.477 6 16H4C4 9.373 9.373 4 16 4s12 5.373 12 12z"
  }), ___EmotionJSX("path", {
    d: "M21.71 11.71L20.3 10.3 18 12.57a4 4 0 00-2-.57 4 4 0 104 4 4 4 0 00-.57-2l2.28-2.29zM16 18a2 2 0 110-4 2 2 0 010 4z"
  }));
};

export var icon = EuiIconAppMetricbeat;