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

var EuiIconInspect = function EuiIconInspect(_ref) {
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
    d: "M15.363 14.658a.5.5 0 11-.713.7l-2.97-3.023a.5.5 0 01.001-.7A3.9 3.9 0 108.9 12.8a.5.5 0 110 .999 4.9 4.9 0 113.821-1.833l2.642 2.691zM3.094 13a.5.5 0 110 1H2.5A2.5 2.5 0 010 11.5v-9A2.5 2.5 0 012.5 0h9A2.5 2.5 0 0114 2.5v.599a.5.5 0 11-1 0V2.5A1.5 1.5 0 0011.5 1h-9A1.5 1.5 0 001 2.5v9A1.5 1.5 0 002.5 13h.594zM2.5 3a.5.5 0 110-1 .5.5 0 010 1zm2 0a.5.5 0 110-1 .5.5 0 010 1zm2 0a.5.5 0 110-1 .5.5 0 010 1zm-4 2a.5.5 0 110-1 .5.5 0 010 1zm2 0a.5.5 0 110-1 .5.5 0 010 1zm-2 1a.5.5 0 110 1 .5.5 0 010-1zm0 3a.5.5 0 110-1 .5.5 0 010 1zm6-6a.5.5 0 110-1 .5.5 0 010 1zm2 0a.5.5 0 110-1 .5.5 0 010 1zm-8 8a.5.5 0 110-1 .5.5 0 010 1z"
  }));
};

export var icon = EuiIconInspect;