/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { EuiThemeAmsterdam, AMSTERDAM_NAME_KEY } from './amsterdam/theme';
import { EuiThemeLegacy, LEGACY_NAME_KEY } from './legacy/theme';
export var EUI_THEMES = [{
  text: 'Light',
  value: 'light',
  provider: EuiThemeAmsterdam
}, {
  text: 'Dark',
  value: 'dark',
  provider: EuiThemeAmsterdam
}, {
  text: 'Legacy: Light',
  value: "".concat(LEGACY_NAME_KEY, "_light"),
  provider: EuiThemeLegacy
}, {
  text: 'Legacy: Dark',
  value: "".concat(LEGACY_NAME_KEY, "_dark"),
  provider: EuiThemeLegacy
}];
export var isLegacyTheme = function isLegacyTheme(name) {
  return name === LEGACY_NAME_KEY;
};
export var isDefaultTheme = function isDefaultTheme(name) {
  return name === AMSTERDAM_NAME_KEY;
};