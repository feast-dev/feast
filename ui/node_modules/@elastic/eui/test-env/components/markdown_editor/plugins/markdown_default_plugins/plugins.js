"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getDefaultEuiMarkdownPlugins = void 0;

var _ui_plugins = require("./ui_plugins");

var _parsing_plugins = require("./parsing_plugins");

var _processing_plugins = require("./processing_plugins");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var getDefaultEuiMarkdownPlugins = function getDefaultEuiMarkdownPlugins(config) {
  return {
    parsingPlugins: (0, _parsing_plugins.getDefaultEuiMarkdownParsingPlugins)(config),
    processingPlugins: (0, _processing_plugins.getDefaultEuiMarkdownProcessingPlugins)(config),
    uiPlugins: (0, _ui_plugins.getDefaultEuiMarkdownUiPlugins)(config)
  };
};

exports.getDefaultEuiMarkdownPlugins = getDefaultEuiMarkdownPlugins;