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
import React from 'react';
import { jsx as ___EmotionJSX } from "@emotion/react";

var MarkdownLogo = function MarkdownLogo(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: "26",
    height: "16",
    fill: "none",
    viewBox: "0 0 26 16",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M1.25 1.875c0-.345.28-.625.625-.625h22.25c.345 0 .625.28.625.625v12.25c0 .345-.28.625-.625.625H1.875a.625.625 0 01-.625-.625V1.875zM1.875 0C.839 0 0 .84 0 1.875v12.25C0 15.161.84 16 1.875 16h22.25C25.16 16 26 15.16 26 14.125V1.875C26 .839 25.16 0 24.125 0H1.875zM3.75 3.75v8.5h2.5V7.375l2.5 3.125 2.5-3.125v4.875h2.5v-8.5h-2.5l-2.5 3.125-2.5-3.125h-2.5zm11.875 4.375l3.75 4.125 3.75-4.125h-2.5V3.75h-2.5v4.375h-2.5z"
  }));
};

export default MarkdownLogo;