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

var EuiIconAppGrok = function EuiIconAppGrok(_ref) {
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
    d: "M9.727 2.91V.97c-1.357 0-4.18 0-5.818 1.94-.97 1.182-1.328 2.908-.97 4.983.563 3.307.97 6.332.301 7.157-.3.31-.97.465-2.24.465v1.94c1.27 0 1.94.155 2.25.494.698.824.262 3.879-.3 7.156-.35 2.124-.01 3.802.96 4.956C5.527 32 8.35 32 9.726 32v-1.94c-1.144 0-3.287 0-4.315-1.221a4.112 4.112 0 01-.533-3.414c.843-5.042.97-7.37-.136-8.727-.068-.077-.145-.136-.213-.203-.068-.068.145-.127.213-.204 1.144-1.367.97-3.695.136-8.727a4.112 4.112 0 01.543-3.414C6.44 2.91 8.583 2.91 9.727 2.91zM32.03 17.454v-1.94c-1.27 0-1.94-.155-2.25-.494-.698-.825-.261-3.88.301-7.157.35-2.123 0-3.801-.97-4.984C27.493.94 24.672.94 23.294.94v1.97c1.145 0 3.288 0 4.315 1.22a4.112 4.112 0 01.544 3.414c-.844 5.043-.97 7.37.135 8.728.068.077.146.135.214.203.067.068-.146.126-.214.204-1.144 1.367-.97 3.694-.135 8.727.329 1.162.13 2.41-.544 3.413-1.018 1.242-3.16 1.242-4.305 1.242V32c1.358 0 4.18 0 5.818-1.94.97-1.182 1.329-2.908.97-4.984-.563-3.306-.97-6.332-.3-7.156.3-.31.969-.465 2.24-.465z"
  }), ___EmotionJSX("path", {
    className: "euiIcon__fillSecondary",
    d: "M26.212 15.515h-2.007a7.758 7.758 0 00-6.72-6.72V6.788h-1.94v2.007a7.758 7.758 0 00-6.72 6.72H6.818v1.94h2.007a7.758 7.758 0 006.72 6.72v2.007h1.94v-2.007a7.758 7.758 0 006.72-6.72h2.007v-1.94zm-8.727 6.7v-1.851h-1.94v1.852a5.818 5.818 0 01-4.76-4.761h1.851v-1.94h-1.852a5.818 5.818 0 014.761-4.761v1.852h1.94v-1.852a5.818 5.818 0 014.761 4.761h-1.852v1.94h1.852a5.818 5.818 0 01-4.761 4.76z"
  }));
};

export var icon = EuiIconAppGrok;