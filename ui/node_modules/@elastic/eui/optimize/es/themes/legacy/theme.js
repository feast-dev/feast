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
import { colors } from '../../global_styling/variables/_colors';
import { base, size } from '../../global_styling/variables/_size';
import { font } from '../../global_styling/variables/_typography';
import { border } from '../../global_styling/variables/_borders';
export var LEGACY_NAME_KEY = 'EUI_THEME_LEGACY';
export var euiThemeLegacy = {
  colors: colors,
  base: base,
  size: size,
  font: font,
  border: border,
  animation: animation,
  breakpoint: breakpoint
};
export var EuiThemeLegacy = buildTheme(euiThemeLegacy, LEGACY_NAME_KEY);