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

var EuiIconTokenNested = function EuiIconTokenNested(_ref) {
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
  }, title) : null, ___EmotionJSX("g", {
    fillRule: "evenodd"
  }, ___EmotionJSX("path", {
    d: "M11 3c1.044 0 1.913.757 1.994 1.736l.006.149v6.23c0 1-.82 1.805-1.845 1.88L11 13H9.501a.5.5 0 01-.09-.992l.09-.008H11c.52 0 .937-.35.993-.783l.007-.102v-6.23c0-.445-.379-.827-.882-.879L11 4H9.5a.5.5 0 01-.09-.992L9.5 3H11zM6.5 3a.5.5 0 01.09.992L6.5 4H5c-.52 0-.937.35-.993.783L4 4.885v6.23c0 .445.379.827.882.879L5 12h1.5a.5.5 0 01.09.992L6.5 13H5c-1.044 0-1.913-.757-1.994-1.736L3 11.115v-6.23c0-1 .82-1.805 1.845-1.88L5 3h1.5z"
  }), ___EmotionJSX("path", {
    d: "M5.864 7.25a.714.714 0 110 1.429.714.714 0 010-1.429zm2.143 0a.714.714 0 110 1.429.714.714 0 010-1.429zm2.143 0a.714.714 0 110 1.429.714.714 0 010-1.429z"
  })));
};

export var icon = EuiIconTokenNested;