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

var EuiIconAggregate = function EuiIconAggregate(_ref) {
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
    d: "M2.5 2a.5.5 0 100 1 .5.5 0 000-1zm0-1a1.5 1.5 0 011.415 1h1.908a1.5 1.5 0 011.393.943L8.839 7H12.5a.5.5 0 010 1H8.839l-1.623 4.057A1.5 1.5 0 015.823 13H3.915a1.5 1.5 0 110-1h1.908a.5.5 0 00.464-.314L7.761 8H3.915a1.5 1.5 0 110-1H7.76L6.287 3.314A.5.5 0 005.823 3H3.915A1.5 1.5 0 112.5 1zm0 11a.5.5 0 110 1 .5.5 0 010-1zM3 7.5a.5.5 0 10-1 0 .5.5 0 001 0zm9.354-3.354a.5.5 0 00-.708.708L13.793 7a.707.707 0 010 1l-2.147 2.146a.5.5 0 00.708.708L14.5 8.707a1.707 1.707 0 000-2.414l-2.146-2.147z"
  }));
};

export var icon = EuiIconAggregate;