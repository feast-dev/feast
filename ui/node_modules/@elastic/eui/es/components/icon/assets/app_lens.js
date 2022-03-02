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

var EuiIconAppLens = function EuiIconAppLens(_ref) {
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
    d: "M23.793 14.293l1.414 1.414-6.408 6.409-3.75-3.25-4.342 4.341-1.414-1.414 5.658-5.659 3.75 3.25 5.092-5.091zM12 11v5l-2 2v-7h2zm10-6v8l-2 2V5h2zm-5 3v7l-2-2V8h2z"
  }), ___EmotionJSX("path", {
    d: "M17 0c8.284 0 15 6.716 15 15 0 8.284-6.716 15-15 15-3.782 0-7.238-1.4-9.876-3.71l-5.417 5.417-1.414-1.414 5.417-5.417A14.943 14.943 0 012 15c0-1.05.108-2.074.313-3.062l1.906.672C4.075 13.385 4 14.184 4 15c0 7.18 5.82 13 13 13s13-5.82 13-13S24.18 2 17 2c-1.002 0-1.978.113-2.915.328l-.75-1.877A15.031 15.031 0 0117 0zM9.621 1.937l.75 1.877a13.062 13.062 0 00-4.82 5.024l-1.907-.673a15.068 15.068 0 015.977-6.228z"
  }));
};

export var icon = EuiIconAppLens;