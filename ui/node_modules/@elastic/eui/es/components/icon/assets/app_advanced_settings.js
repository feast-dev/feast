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

var EuiIconAppAdvancedSettings = function EuiIconAppAdvancedSettings(_ref) {
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
    d: "M2.909 26.182h1.939v4.848H2.909z"
  }), ___EmotionJSX("path", {
    d: "M4.848 16.62V0H2.91v16.62a3.879 3.879 0 101.94 0zm-.97 5.683a1.94 1.94 0 110-3.879 1.94 1.94 0 010 3.879z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M14.545 16.485h1.939V31.03h-1.939z"
  }), ___EmotionJSX("path", {
    d: "M16.485 6.924V0h-1.94v6.924a3.879 3.879 0 101.94 0zm-.97 5.682a1.94 1.94 0 110-3.879 1.94 1.94 0 010 3.88z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M26.182 26.182h1.939v4.848h-1.939z"
  }), ___EmotionJSX("path", {
    d: "M28.121 16.62V0h-1.94v16.62a3.879 3.879 0 101.94 0zm-.97 5.683a1.94 1.94 0 110-3.879 1.94 1.94 0 010 3.879z"
  }));
};

export var icon = EuiIconAppAdvancedSettings;