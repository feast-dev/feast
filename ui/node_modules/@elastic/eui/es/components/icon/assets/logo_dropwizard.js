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
import { htmlIdGenerator } from '../../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";

var EuiIconLogoDropwizard = function EuiIconLogoDropwizard(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  var generateId = htmlIdGenerator('logo_dropwizard');
  return ___EmotionJSX("svg", _extends({
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    fill: "none",
    xmlns: "http://www.w3.org/2000/svg",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M8 29.61s9.55 4.74 16.856-.893c-1.905-5.114-5.25-19.867-7.226-26.383-.422-1.389-.49-1.527-.663-1.909-.964-2.134-1.978 4.142-3.628 10.459C12.749 13.148 8 29.61 8 29.61",
    fill: "url(#".concat(generateId('a'), ")")
  }), ___EmotionJSX("path", {
    d: "M9.926 28.563s8.17 4.794 14.93.154c-.18-.807-.642-2.103-.955-3.311-1.664-6.416-4.676-17.248-6.144-22.626-1.59-5.822-2.105.986-3.961 8.23-.564 2.204-3.87 17.553-3.87 17.553",
    fill: "url(#".concat(generateId('b'), ")")
  }), ___EmotionJSX("path", {
    d: "M12.14 24.41l.85-1.466.793 1.544-.908 1.99-.735-2.069zm6.934-5.545l.85-1.465.793 1.543-.908 1.99-.735-2.068zm-4.747-5.851l.85-1.465.792 1.543-.908 1.99-.734-2.068",
    fill: "#F9A72B"
  }), ___EmotionJSX("path", {
    d: "M12.14 24.41l.85-1.716.793 1.794-.907 1.468-.736-1.546zm6.934-5.545l.851-1.716.792 1.794-.906 1.468-.737-1.546zm-4.747-5.851l.85-1.716.792 1.794-.906 1.468-.736-1.546",
    fill: "#FFF200"
  }), ___EmotionJSX("path", {
    d: "M12.987 28.422s6.082 1.015 10.086-1.549c-.502 1.55-4.991 3.314-10.086 1.55",
    fill: "#24265D"
  }), ___EmotionJSX("defs", null, ___EmotionJSX("linearGradient", {
    id: generateId('a'),
    x1: 33.473,
    y1: 7.674,
    x2: 7.751,
    y2: 21.331,
    gradientUnits: "userSpaceOnUse"
  }, ___EmotionJSX("stop", {
    stopColor: "#3871C1"
  }), ___EmotionJSX("stop", {
    offset: 0.515,
    stopColor: "#2C3792"
  }), ___EmotionJSX("stop", {
    offset: 0.865,
    stopColor: "#24265D"
  }), ___EmotionJSX("stop", {
    offset: 1,
    stopColor: "#252761"
  })), ___EmotionJSX("linearGradient", {
    id: generateId('b'),
    x1: 21.028,
    y1: 14.928,
    x2: 6.017,
    y2: 18.844,
    gradientUnits: "userSpaceOnUse"
  }, ___EmotionJSX("stop", {
    stopColor: "#3871C1"
  }), ___EmotionJSX("stop", {
    offset: 0.515,
    stopColor: "#2C3792"
  }), ___EmotionJSX("stop", {
    offset: 0.865,
    stopColor: "#24265D"
  }), ___EmotionJSX("stop", {
    offset: 1,
    stopColor: "#252761"
  }))));
};

export var icon = EuiIconLogoDropwizard;