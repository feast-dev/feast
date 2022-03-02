/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { getDefaultEuiMarkdownUiPlugins } from './ui_plugins';
import { getDefaultEuiMarkdownParsingPlugins } from './parsing_plugins';
import { getDefaultEuiMarkdownProcessingPlugins } from './processing_plugins';
export var getDefaultEuiMarkdownPlugins = function getDefaultEuiMarkdownPlugins(config) {
  return {
    parsingPlugins: getDefaultEuiMarkdownParsingPlugins(config),
    processingPlugins: getDefaultEuiMarkdownProcessingPlugins(config),
    uiPlugins: getDefaultEuiMarkdownUiPlugins(config)
  };
};