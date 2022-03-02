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

var EuiIconCalendar = function EuiIconCalendar(_ref) {
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
    d: "M14 4v-.994C14 2.45 13.55 2 12.994 2H11v1h-1V2H6v1H5V2H3.006C2.45 2 2 2.45 2 3.006v9.988C2 13.55 2.45 14 3.006 14h9.988C13.55 14 14 13.55 14 12.994V5H2V4h12zm-3-3h1.994C14.102 1 15 1.897 15 3.006v9.988A2.005 2.005 0 0112.994 15H3.006A2.005 2.005 0 011 12.994V3.006C1 1.898 1.897 1 3.006 1H5V0h1v1h4V0h1v1zM4 7h2v1H4V7zm3 0h2v1H7V7zm3 0h2v1h-2V7zM4 9h2v1H4V9zm3 0h2v1H7V9zm3 0h2v1h-2V9zm-6 2h2v1H4v-1zm3 0h2v1H7v-1zm3 0h2v1h-2v-1z",
    fillRule: "evenodd"
  }));
};

export var icon = EuiIconCalendar;