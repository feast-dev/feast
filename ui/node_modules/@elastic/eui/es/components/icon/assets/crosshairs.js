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

var EuiIconCrosshairs = function EuiIconCrosshairs(_ref) {
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
    d: "M5.822 1.874a.5.5 0 11.335.942 5.517 5.517 0 00-3.34 3.341.5.5 0 11-.943-.335 6.517 6.517 0 013.948-3.948zM1.864 10.15a.5.5 0 11.944-.33 5.517 5.517 0 003.365 3.37.5.5 0 01-.333.943 6.517 6.517 0 01-3.976-3.983zm8.302 3.981a.5.5 0 11-.333-.943 5.517 5.517 0 003.347-3.332.5.5 0 11.941.337 6.517 6.517 0 01-3.955 3.938zm3.968-8.285a.5.5 0 11-.943.331A5.517 5.517 0 009.85 2.82a.5.5 0 01.337-.942 6.517 6.517 0 013.946 3.968zM8.5 3.5a.5.5 0 01-1 0V.997a.5.5 0 011 0V3.5zm-4.997 4a.5.5 0 010 1H1a.5.5 0 010-1h2.503zM7.5 12.497a.5.5 0 011 0V15a.5.5 0 11-1 0v-2.503zM12.497 8.5a.5.5 0 010-1H15a.5.5 0 110 1h-2.503zM8 9a1 1 0 110-2 1 1 0 010 2z"
  }));
};

export var icon = EuiIconCrosshairs;