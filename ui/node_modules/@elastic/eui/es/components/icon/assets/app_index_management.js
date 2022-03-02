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

var EuiIconAppIndexManagement = function EuiIconAppIndexManagement(_ref) {
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
    d: "M17 18v-2h-2v2H3v6h2v-4h10v4h2v-4h10v4h2v-6z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M4 32a3 3 0 110-6 3 3 0 010 6zm0-4a1 1 0 100 2 1 1 0 000-2zm12 4a3 3 0 110-6 3 3 0 010 6zm0-4a1 1 0 100 2 1 1 0 000-2zm12 4a3 3 0 110-6 3 3 0 010 6zm0-4a1 1 0 100 2 1 1 0 000-2zM23 8V6h-2.1a5 5 0 00-.73-1.75l1.49-1.49-1.42-1.42-1.49 1.49A5 5 0 0017 2.1V0h-2v2.1a5 5 0 00-1.75.73l-1.49-1.49-1.42 1.42 1.49 1.49A5 5 0 0011.1 6H9v2h2.1a5 5 0 00.73 1.75l-1.49 1.49 1.41 1.41 1.49-1.49a5 5 0 001.76.74V14h2v-2.1a5 5 0 001.75-.73l1.49 1.49 1.41-1.41-1.48-1.5A5 5 0 0020.9 8H23zm-7 2a3 3 0 110-6 3 3 0 010 6z"
  }), ___EmotionJSX("path", {
    d: "M16 8a1 1 0 01-1-1 1.39 1.39 0 010-.2.65.65 0 01.06-.18.74.74 0 01.09-.18 1.61 1.61 0 01.12-.15.93.93 0 01.33-.21 1 1 0 011.09.21l.12.15a.78.78 0 01.09.18.62.62 0 01.1.18 1.27 1.27 0 010 .2 1 1 0 01-1 1z"
  }));
};

export var icon = EuiIconAppIndexManagement;