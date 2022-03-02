function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

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

var EuiIconMlCreateMultiMetricJob = function EuiIconMlCreateMultiMetricJob(_ref) {
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
    d: "M4 24a4 4 0 110 8 4 4 0 010-8zm24 0a4 4 0 110 8 4 4 0 010-8zM4 26a2 2 0 100 4 2 2 0 000-4zm24 0a2 2 0 100 4 2 2 0 000-4zM17 9v6h6v2h-6v6h-2v-6H9v-2h6V9h2zM4 0a4 4 0 110 8 4 4 0 010-8zm24 0a4 4 0 110 8 4 4 0 010-8zm0 2a2 2 0 100 4 2 2 0 000-4zM4 2a2 2 0 100 4 2 2 0 000-4z"
  }), ___EmotionJSX("path", {
    d: "M21.078 29.05c.102.677.3 1.322.582 1.92A15.963 15.963 0 0116 32c-1.993 0-3.9-.364-5.66-1.03.281-.598.48-1.243.582-1.92 1.574.614 3.287.95 5.078.95 1.791 0 3.504-.336 5.078-.95zm9.892-18.71A15.963 15.963 0 0132 16c0 1.993-.364 3.9-1.03 5.66a6.948 6.948 0 00-1.92-.582c.614-1.574.95-3.287.95-5.078 0-1.791-.336-3.504-.95-5.078a6.948 6.948 0 001.92-.582zm-29.94 0c.598.281 1.243.48 1.92.582A13.965 13.965 0 002 16c0 1.791.336 3.504.95 5.078-.677.102-1.322.3-1.92.582A15.964 15.964 0 010 16c0-1.993.364-3.9 1.03-5.66zM16 0c1.993 0 3.9.364 5.66 1.03a6.948 6.948 0 00-.582 1.92A13.965 13.965 0 0016 2c-1.791 0-3.504.336-5.078.95a6.948 6.948 0 00-.582-1.92A15.964 15.964 0 0116 0z"
  }));
};

export var icon = EuiIconMlCreateMultiMetricJob;