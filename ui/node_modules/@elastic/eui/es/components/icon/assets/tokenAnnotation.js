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

var EuiIconTokenAnnotation = function EuiIconTokenAnnotation(_ref) {
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
    d: "M8.15 3.392c2.797 0 4.524 1.644 4.517 4.289.007 1.816-.708 2.893-2.21 3.004-.908.076-1.081-.287-1.157-.725h-.041c-.163.42-.964.732-1.744.683-1.053-.065-2.082-.842-2.09-2.572.008-1.72 1.071-2.441 1.959-2.586.804-.135 1.598.158 1.723.462h.051v-.386h1.195v3.452c.007.3.128.425.304.425.4 0 .677-.583.673-1.861.004-2.376-1.705-2.914-3.187-2.914-2.34 0-3.415 1.522-3.422 3.387.007 2.127 1.22 3.277 3.433 3.277.808 0 1.598-.176 2.006-.349l.393 1.122c-.435.27-1.419.508-2.493.508-2.98 0-4.723-1.66-4.727-4.496.004-2.804 1.748-4.72 4.817-4.72zM7.964 6.79c-.76 0-1.185.459-1.188 1.24.003.683.3 1.332 1.202 1.332.821 0 1.094-.473 1.077-1.343-.004-.718-.204-1.23-1.091-1.23z"
  }));
};

export var icon = EuiIconTokenAnnotation;