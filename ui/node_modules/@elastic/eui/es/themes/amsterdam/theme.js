/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { buildTheme } from '../../services/theme/utils';
import { animation } from '../../global_styling/variables/_animations';
import { breakpoint } from '../../global_styling/variables/_breakpoint';
import { base, size } from '../../global_styling/variables/_size';
import { colors_ams } from './global_styling/variables/_colors';
import { font_ams } from './global_styling/variables/_typography';
import { border_ams } from './global_styling/variables/_borders';
export var AMSTERDAM_NAME_KEY = 'EUI_THEME_AMSTERDAM';
export var euiThemeAmsterdam = {
  colors: colors_ams,
  base: base,
  size: size,
  font: font_ams,
  border: border_ams,
  animation: animation,
  breakpoint: breakpoint
};
export var EuiThemeAmsterdam = buildTheme(euiThemeAmsterdam, AMSTERDAM_NAME_KEY);