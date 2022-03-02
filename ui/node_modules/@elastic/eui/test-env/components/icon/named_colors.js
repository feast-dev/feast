"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isNamedColor = isNamedColor;
exports.colorToClassMap = void 0;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var colorToClassMap = {
  default: null,
  primary: 'euiIcon--primary',
  success: 'euiIcon--success',
  accent: 'euiIcon--accent',
  warning: 'euiIcon--warning',
  danger: 'euiIcon--danger',
  text: 'euiIcon--text',
  subdued: 'euiIcon--subdued',
  ghost: 'euiIcon--ghost',
  inherit: 'euiIcon--inherit'
};
exports.colorToClassMap = colorToClassMap;

function isNamedColor(name) {
  return colorToClassMap.hasOwnProperty(name);
}