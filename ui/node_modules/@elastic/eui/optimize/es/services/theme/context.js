/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { createContext } from 'react';
import { EuiThemeAmsterdam } from '../../themes/amsterdam/theme';
import { DEFAULT_COLOR_MODE, getComputed } from './utils';
export var EuiSystemContext = /*#__PURE__*/createContext(EuiThemeAmsterdam);
export var EuiModificationsContext = /*#__PURE__*/createContext({});
export var EuiColorModeContext = /*#__PURE__*/createContext(DEFAULT_COLOR_MODE);
export var EuiThemeContext = /*#__PURE__*/createContext(getComputed(EuiThemeAmsterdam, {}, DEFAULT_COLOR_MODE));