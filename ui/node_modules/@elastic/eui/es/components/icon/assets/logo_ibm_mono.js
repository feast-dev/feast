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

var EuiIconLogoIbmMono = function EuiIconLogoIbmMono(_ref) {
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
    fillRule: "evenodd",
    d: "M.047 22.909l1.186.009a5.69 5.69 0 001.66 4.055 5.802 5.802 0 004.071 1.705h16.618c2.78.002 5.302-1.616 6.438-4.13a6.929 6.929 0 00-1.18-7.505l.897-.795a8.112 8.112 0 011.384 8.785c-1.329 2.943-4.279 4.837-7.534 4.838H6.96a7.012 7.012 0 01-4.922-2.06A6.876 6.876 0 01.029 22.91h.018-.018c.017-2.71 1.62-5.091 3.997-6.2.114-.054.32.29.619 1.03a5.71 5.71 0 00-3.412 5.18l-1.186-.01zm4.215-9.787a.604.604 0 01-.3-.08l-2.844-1.625a.617.617 0 01-.295-.734.608.608 0 01.868-.315l2.843 1.625c.29.159.405.514.261.81a.603.603 0 01-.533.32zm4.914-4.864a.603.603 0 01-.522-.298L7.012 5.144a.615.615 0 01.115-.782.606.606 0 01.91.157l1.64 2.813a.617.617 0 01-.158.819c-.1.07-.22.107-.343.107zm6.107-2.416V2.635a.624.624 0 01.575-.634.605.605 0 01.443.164.593.593 0 01.186.431v3.285a.593.593 0 01-.186.43.605.605 0 01-.443.165.624.624 0 01-.575-.634zm7.312 2.416a.604.604 0 01-.343-.107.617.617 0 01-.159-.82l1.64-2.812a.602.602 0 01.911-.157c.223.2.271.528.115.782L23.117 7.96a.603.603 0 01-.522.298zm4.914 4.864a.602.602 0 01-.534-.319.616.616 0 01.262-.81l2.818-1.611a.626.626 0 01.783.112.592.592 0 01-.156.906l-2.873 1.643a.604.604 0 01-.3.08zM16.366 19.78H16a2.322 2.322 0 01-.302 0c-.09-.011-.152.05-.086-.196a8.204 8.204 0 015.057-5.533 8.308 8.308 0 019.068 2.197l-.897.795a7.097 7.097 0 00-7.745-1.876 7.009 7.009 0 00-4.288 4.613c-.115.399-.115.399 0 0h-.44zM6.154 23.826a10.626 10.626 0 012.092-12.22c4.219-4.178 11.06-4.178 15.279 0l-.851.843c-3.75-3.713-9.828-3.713-13.577 0A9.442 9.442 0 007.264 23.36a6.99 6.99 0 00-.418.172l-.212.094-.162.077-.094.03-.09.039c.648-.282.965-.42.95-.412-.026.015-1.073.489-1.084.466z"
  }));
};

export var icon = EuiIconLogoIbmMono;