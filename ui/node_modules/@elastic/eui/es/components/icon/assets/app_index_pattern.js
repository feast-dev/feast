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

var EuiIconAppIndexPattern = function EuiIconAppIndexPattern(_ref) {
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
    d: "M20 14h-8a3 3 0 01-3-3V3a3 3 0 013-3h8a3 3 0 013 3v8a3 3 0 01-3 3zM12 2a1 1 0 00-1 1v8a1 1 0 001 1h8a1 1 0 001-1V3a1 1 0 00-1-1h-8zM17 18v-2h-2v2H3v6h2v-4h10v4h2v-4h10v4h2v-6z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M4 32a3 3 0 110-6 3 3 0 010 6zm0-4a1 1 0 100 2 1 1 0 000-2zm12 4a3 3 0 110-6 3 3 0 010 6zm0-4a1 1 0 100 2 1 1 0 000-2zm12 4a3 3 0 110-6 3 3 0 010 6zm0-4a1 1 0 100 2 1 1 0 000-2zM13 4h6v2h-6zM13 8h6v2h-6z"
  }));
};

export var icon = EuiIconAppIndexPattern;