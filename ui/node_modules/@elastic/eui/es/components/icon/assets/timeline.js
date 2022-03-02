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

var EuiIconTimeline = function EuiIconTimeline(_ref) {
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
    fillRule: "evenodd",
    d: "M7 4.5a.5.5 0 001 0V4h1a1 1 0 001-1V1a1 1 0 00-1-1H6a1 1 0 00-1 1v2a1 1 0 001 1h1v.5zM9 1H6v2h3V1zM2 7.5a.5.5 0 111 0 .5.5 0 01-1 0zM2.5 9a1.5 1.5 0 01-1.415-1H.5a.5.5 0 010-1h.585a1.5 1.5 0 012.83 0h2.17a1.5 1.5 0 012.83 0h2.17a1.5 1.5 0 012.83 0h.585a.5.5 0 010 1h-.585a1.5 1.5 0 01-2.83 0h-2.17a1.5 1.5 0 01-2.83 0h-2.17A1.5 1.5 0 012.5 9zM13 7.5a.5.5 0 10-1 0 .5.5 0 001 0zm-5 0a.5.5 0 10-1 0 .5.5 0 001 0zM2.5 10a.5.5 0 00-.5.5v.5H1a1 1 0 00-1 1v2a1 1 0 001 1h3a1 1 0 001-1v-2a1 1 0 00-1-1H3v-.5a.5.5 0 00-.5-.5zM4 14v-2H1v2h3zm8-3.5a.5.5 0 011 0v.5h1a1 1 0 011 1v2a1 1 0 01-1 1h-3a1 1 0 01-1-1v-2a1 1 0 011-1h1v-.5zm2 2.5v1h-3v-2h3v1z"
  }));
};

export var icon = EuiIconTimeline;