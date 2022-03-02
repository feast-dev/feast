/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import * as MarkdownTooltip from '../markdown_tooltip';
export var getDefaultEuiMarkdownUiPlugins = function getDefaultEuiMarkdownUiPlugins() {
  var _ref = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      exclude = _ref.exclude;

  var excludeSet = new Set(exclude);
  var uiPlugins = [];
  if (!excludeSet.has('tooltip')) uiPlugins.push(MarkdownTooltip.plugin); // @ts-ignore __originatedFromEui is a custom property

  uiPlugins.__originatedFromEui = true;
  return uiPlugins;
};
export var defaultUiPlugins = getDefaultEuiMarkdownUiPlugins();