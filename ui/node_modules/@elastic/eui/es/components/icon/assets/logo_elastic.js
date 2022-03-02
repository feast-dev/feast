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

var EuiIconLogoElastic = function EuiIconLogoElastic(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    "data-type": "logoElastic",
    xmlns: "http://www.w3.org/2000/svg",
    width: 32,
    height: 32,
    fill: "none",
    viewBox: "0 0 32 32",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    className: "outline",
    fillRule: "evenodd",
    clipRule: "evenodd",
    d: "M30.86 13.129A6.333 6.333 0 0132 16.77a6.419 6.419 0 01-1.162 3.652 6.382 6.382 0 01-3.032 2.331 4.833 4.833 0 01-1.672 5.542 4.789 4.789 0 01-5.77-.074 9.016 9.016 0 01-4.5 3.322 8.982 8.982 0 01-5.587.023 9.014 9.014 0 01-4.526-3.287 9.067 9.067 0 01-1.727-5.333c0-.579.053-1.156.16-1.725A6.305 6.305 0 011.145 18.9 6.341 6.341 0 010 15.242a6.42 6.42 0 011.163-3.652 6.384 6.384 0 013.035-2.33 4.833 4.833 0 011.658-5.557 4.789 4.789 0 015.78.07 9.037 9.037 0 014.93-3.446 9.007 9.007 0 015.994.419 9.05 9.05 0 014.407 4.098 9.097 9.097 0 01.873 5.965 6.298 6.298 0 013.02 2.32zm-18.28.658l7.002 3.211 7.066-6.213a7.85 7.85 0 00.152-1.557c0-1.692-.539-3.34-1.54-4.704a7.897 7.897 0 00-4.02-2.869 7.87 7.87 0 00-4.932.086 7.9 7.9 0 00-3.92 3.007l-1.174 6.118 1.367 2.92-.001.001zm-7.247 7.441A7.964 7.964 0 006.72 27.53a7.918 7.918 0 004.04 2.874 7.89 7.89 0 004.95-.097 7.92 7.92 0 003.926-3.03l1.166-6.102-1.555-2.985-7.03-3.211-6.885 6.248.001.001zm4.755-11.024l-4.8-1.137.002-.002a3.82 3.82 0 011.312-4.358 3.785 3.785 0 014.538.023l-1.052 5.474zm-5.216.01a5.294 5.294 0 00-2.595 1.882 5.324 5.324 0 00-.142 6.124 5.287 5.287 0 002.505 2l6.733-6.101-1.235-2.65-5.266-1.255zm18.286 17.848a3.737 3.737 0 01-2.285-.785l1.037-5.454 4.8 1.125a3.812 3.812 0 01-1.801 4.68c-.54.283-1.14.432-1.751.434zm-1.31-7.499l5.28 1.238a5.34 5.34 0 002.622-1.938 5.37 5.37 0 001.013-3.106 5.311 5.311 0 00-.936-3.01 5.282 5.282 0 00-2.475-1.944l-6.904 6.07 1.4 2.69z",
    fill: "#fff"
  }), ___EmotionJSX("path", {
    d: "M12.58 13.787l7.002 3.211 7.066-6.213a7.849 7.849 0 00.152-1.557c0-1.692-.539-3.34-1.54-4.704a7.897 7.897 0 00-4.02-2.869 7.87 7.87 0 00-4.932.086 7.9 7.9 0 00-3.92 3.007l-1.174 6.118 1.367 2.92-.001.001z",
    fill: "#FEC514"
  }), ___EmotionJSX("path", {
    d: "M5.333 21.228A7.964 7.964 0 006.72 27.53a7.918 7.918 0 004.04 2.874 7.89 7.89 0 004.95-.097 7.92 7.92 0 003.926-3.03l1.166-6.102-1.555-2.985-7.03-3.211-6.885 6.248.001.001z",
    fill: "#00BFB3"
  }), ___EmotionJSX("path", {
    d: "M5.288 9.067l4.8 1.137L11.14 4.73a3.785 3.785 0 00-5.914 1.94 3.82 3.82 0 00.064 2.395",
    fill: "#F04E98"
  }), ___EmotionJSX("path", {
    d: "M4.872 10.214a5.294 5.294 0 00-2.595 1.882 5.324 5.324 0 00-.142 6.124 5.287 5.287 0 002.505 2l6.733-6.101-1.235-2.65-5.266-1.255z",
    fill: "#1BA9F5"
  }), ___EmotionJSX("path", {
    d: "M20.873 27.277a3.736 3.736 0 002.285.785 3.783 3.783 0 003.101-1.63 3.812 3.812 0 00.451-3.484l-4.8-1.125-1.037 5.454z",
    fill: "#93C90E"
  }), ___EmotionJSX("path", {
    d: "M21.848 20.563l5.28 1.238a5.34 5.34 0 002.622-1.938 5.37 5.37 0 001.013-3.106 5.312 5.312 0 00-.936-3.01 5.283 5.283 0 00-2.475-1.944l-6.904 6.07 1.4 2.69z",
    fill: "#07C"
  }));
};

export var icon = EuiIconLogoElastic;