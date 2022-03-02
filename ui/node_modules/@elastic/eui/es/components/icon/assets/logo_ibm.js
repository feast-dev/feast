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

var EuiIconLogoIbm = function EuiIconLogoIbm(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  var generateId = htmlIdGenerator('logo_ibm');
  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("defs", null, ___EmotionJSX("linearGradient", {
    id: generateId('a'),
    x1: "8.005%",
    x2: "53.33%",
    y1: "79.323%",
    y2: "79.324%"
  }, ___EmotionJSX("stop", {
    offset: "0%",
    stopColor: "#3BDADE",
    stopOpacity: 0
  }), ___EmotionJSX("stop", {
    offset: "30.476%",
    stopColor: "#36CEE1",
    stopOpacity: 0.383
  }), ___EmotionJSX("stop", {
    offset: "100%",
    stopColor: "#21A2EF"
  })), ___EmotionJSX("linearGradient", {
    id: generateId('b'),
    x1: "19.496%",
    x2: "71.309%",
    y1: "83.344%",
    y2: "17.638%"
  }, ___EmotionJSX("stop", {
    offset: "0%",
    stopColor: "#207FFA"
  }), ___EmotionJSX("stop", {
    offset: "100%",
    stopColor: "#6EAFF1",
    stopOpacity: 0
  })), ___EmotionJSX("linearGradient", {
    id: generateId('c'),
    x1: "21.909%",
    x2: "21.909%",
    y1: "96.314%",
    y2: "29.454%"
  }, ___EmotionJSX("stop", {
    offset: "0%",
    stopColor: "#42C1E6",
    stopOpacity: 0
  }), ___EmotionJSX("stop", {
    offset: "57.076%",
    stopColor: "#6FB9EF"
  }), ___EmotionJSX("stop", {
    offset: "86.716%",
    stopColor: "#60CBE7"
  }), ___EmotionJSX("stop", {
    offset: "100%",
    stopColor: "#5CCFE5",
    stopOpacity: 0
  })), ___EmotionJSX("linearGradient", {
    id: generateId('d'),
    x1: "96.205%",
    x2: "-1.172%",
    y1: "12.667%",
    y2: "-22.252%"
  }, ___EmotionJSX("stop", {
    offset: "0%",
    stopColor: "#6FB9EF",
    stopOpacity: 0
  }), ___EmotionJSX("stop", {
    offset: "22.427%",
    stopColor: "#38D2E0"
  }), ___EmotionJSX("stop", {
    offset: "91.203%",
    stopColor: "#6ABFEC"
  }), ___EmotionJSX("stop", {
    offset: "100%",
    stopColor: "#5CCFE5",
    stopOpacity: 0
  })), ___EmotionJSX("linearGradient", {
    id: generateId('e'),
    x1: "9.408%",
    x2: "80.589%",
    y1: "87.764%",
    y2: "83.138%"
  }, ___EmotionJSX("stop", {
    offset: "0%",
    stopColor: "#126CFD"
  }), ___EmotionJSX("stop", {
    offset: "100%",
    stopColor: "#21A1EF"
  }))), ___EmotionJSX("g", {
    fill: "none",
    fillRule: "evenodd"
  }, ___EmotionJSX("path", {
    fill: "#38D2E0",
    d: "M4.262 13.122a.604.604 0 01-.3-.08l-2.844-1.625a.617.617 0 01-.295-.734.608.608 0 01.868-.315l2.843 1.625c.29.159.405.514.261.81a.603.603 0 01-.533.32zm4.914-4.864a.603.603 0 01-.522-.298L7.012 5.144a.615.615 0 01.115-.782.606.606 0 01.91.157l1.64 2.813a.617.617 0 01-.158.819c-.1.07-.22.107-.343.107zm6.107-2.416V2.635a.624.624 0 01.575-.634.605.605 0 01.443.164.593.593 0 01.186.431v3.285a.593.593 0 01-.186.43.605.605 0 01-.443.165.624.624 0 01-.575-.634zm7.312 2.416a.604.604 0 01-.343-.107.617.617 0 01-.159-.82l1.64-2.812a.602.602 0 01.911-.157c.223.2.271.528.115.782L23.117 7.96a.603.603 0 01-.522.298zm4.914 4.864a.602.602 0 01-.534-.319.616.616 0 01.262-.81l2.818-1.611a.626.626 0 01.783.112.592.592 0 01-.156.906l-2.873 1.643a.604.604 0 01-.3.08z"
  }), ___EmotionJSX("path", {
    fill: "url(#".concat(generateId('a'), ")"),
    d: "M16.536 19.645l-1.204-.009c.026-3.377 2.146-6.391 5.337-7.585a8.308 8.308 0 019.068 2.197l-.897.795a7.097 7.097 0 00-7.745-1.876c-2.726 1.019-4.537 3.593-4.559 6.478z",
    transform: "translate(0 2)"
  }), ___EmotionJSX("path", {
    fill: "url(#".concat(generateId('b'), ")"),
    d: "M.029 20.909c.022-3.354 2.475-6.208 5.815-6.764l.201 1.176c-2.764.46-4.794 2.82-4.812 5.596L.029 20.91z",
    transform: "translate(0 2)"
  }), ___EmotionJSX("path", {
    fill: "url(#".concat(generateId('c'), ")"),
    d: "M8.246 24.737a10.625 10.625 0 010-15.131c4.219-4.178 11.06-4.178 15.279 0a10.89 10.89 0 011.384 1.678l-1.004.657a9.517 9.517 0 00-1.231-1.492c-3.75-3.713-9.828-3.713-13.577 0a9.442 9.442 0 000 13.445l-.851.843z",
    transform: "translate(0 2)"
  }), ___EmotionJSX("path", {
    fill: "url(#".concat(generateId('d'), ")"),
    d: "M5.836 13.234c.404-1.332 1.33-2.56 2.41-3.628 4.219-4.178 11.06-4.178 15.279 0a10.89 10.89 0 011.384 1.678l-1.004.657a9.517 9.517 0 00-1.231-1.492c-3.75-3.713-9.828-3.713-13.577 0a9.474 9.474 0 00-2.074 3.06c-1.452 3.45-2.358 3.593-1.187-.275z",
    transform: "translate(0 2)"
  }), ___EmotionJSX("path", {
    fill: "url(#".concat(generateId('e'), ")"),
    d: "M23.524 27.87H6.96a7.012 7.012 0 01-4.922-2.059A6.876 6.876 0 01.029 20.91l1.204.009a5.69 5.69 0 001.66 4.055 5.802 5.802 0 004.071 1.705h16.618c2.78.002 5.302-1.616 6.438-4.13a6.929 6.929 0 00-1.18-7.505l.897-.795a8.112 8.112 0 011.384 8.785c-1.329 2.943-4.279 4.837-7.534 4.838h-.063z",
    transform: "translate(0 2)"
  })));
};

export var icon = EuiIconLogoIbm;