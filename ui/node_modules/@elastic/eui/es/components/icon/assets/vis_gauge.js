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

var EuiIconVisGauge = function EuiIconVisGauge(_ref) {
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
    d: "M12.877 5.847l-1.02 1.02a.5.5 0 01-.708-.707l1.1-1.099c-.05-.053-.1-.106-.152-.157A6.471 6.471 0 008 3.019V4.5a.5.5 0 01-1 0V3.019a6.47 6.47 0 00-4.261 2.055l1.07 1.071a.5.5 0 01-.706.707l-.99-.99A6.46 6.46 0 001.018 10H2.5a.5.5 0 110 1H1.174c.083.353.196.697.337 1.03a.5.5 0 11-.922.39A7.487 7.487 0 010 9.5a7.483 7.483 0 012.197-5.304A7.487 7.487 0 017.5 2a7.487 7.487 0 015.304 2.197A7.483 7.483 0 0115 9.5a7.487 7.487 0 01-.59 2.92.5.5 0 01-.92-.39c.14-.333.253-.677.336-1.03H12.5a.5.5 0 110-1h1.481a6.483 6.483 0 00-1.104-4.153zm-6.041 5.317a.993.993 0 01-.01-1.404c.384-.385 2.882-2.002 3.149-1.735.267.267-1.35 2.765-1.735 3.15a.993.993 0 01-1.404-.01z"
  }));
};

export var icon = EuiIconVisGauge;