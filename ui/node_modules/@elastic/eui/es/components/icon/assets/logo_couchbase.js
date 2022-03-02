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

var EuiIconLogoCouchbase = function EuiIconLogoCouchbase(_ref) {
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
    fill: "#ED2226",
    d: "M16.072.024C7.29.024.144 7.146.144 15.952c0 8.782 7.122 15.928 15.928 15.928C24.854 31.88 32 24.758 32 15.952 32 7.146 24.854.024 16.072.024zm10.755 18.719c0 .962-.553 1.804-1.636 1.997-1.877.336-5.823.53-9.119.53-3.296 0-7.242-.194-9.119-.53-1.082-.193-1.636-1.035-1.636-1.997v-6.208c0-.962.746-1.852 1.636-1.997.554-.096 1.853-.192 2.864-.192.385 0 .697.289.697.746v4.355l5.582-.12 5.582.12v-4.355c0-.457.313-.746.698-.746 1.01 0 2.31.096 2.863.192.914.145 1.636 1.035 1.636 1.997-.048 2.045-.048 4.139-.048 6.208z"
  }));
};

export var icon = EuiIconLogoCouchbase;