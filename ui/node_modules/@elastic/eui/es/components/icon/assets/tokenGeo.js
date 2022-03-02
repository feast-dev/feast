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

var EuiIconTokenGeo = function EuiIconTokenGeo(_ref) {
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
    fillRule: "evenodd",
    d: "M8 3c1.636 0 3.088.785 4 2 .628.836 1 1.875 1 3a4.978 4.978 0 01-.999 3H12a4.992 4.992 0 01-4 2 4.992 4.992 0 01-4-1.999V11a5 5 0 014-8zm.948 8H7.052c.277.626.623 1 .948 1 .325 0 .67-.374.948-1zM6 11l-.645.001c.274.242.581.446.914.606A5.445 5.445 0 016 11.001zm4.645.001H10a5.51 5.51 0 01-.269.606c.333-.16.64-.364.914-.606zm-5.133-2.5H4.031c.068.54.243 1.048.505 1.5h1.172a9.186 9.186 0 01-.196-1.5zm3.975 0H6.513c.03.544.104 1.05.21 1.5h2.553c.107-.45.182-.956.21-1.5zm2.482 0h-1.481a9.186 9.186 0 01-.196 1.5h1.172c.262-.452.437-.96.505-1.5zM5.708 6L4.535 6c-.261.452-.437.96-.504 1.5h1.481A9.187 9.187 0 015.708 6zm3.568 0H6.724c-.107.449-.182.955-.21 1.499h2.973a8.479 8.479 0 00-.21-1.5zM11.465 6h-1.173c.102.467.17.972.196 1.5h1.481a3.974 3.974 0 00-.504-1.5zM6.269 4.393l-.124.062c-.286.15-.551.333-.79.545H6a5.51 5.51 0 01.269-.607zM8 4c-.326 0-.671.375-.948 1h1.896C8.671 4.376 8.326 4 8 4zm1.73.393l.038.071c.083.168.161.347.232.536h.646a4.006 4.006 0 00-.915-.607z"
  }));
};

export var icon = EuiIconTokenGeo;