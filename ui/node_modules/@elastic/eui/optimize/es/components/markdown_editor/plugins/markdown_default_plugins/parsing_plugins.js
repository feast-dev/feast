/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// Importing seemingly unused types from `unified` because the definitions
// are exported for two versions of TypeScript (3.4, 4.0) and implicit
// imports during eui.d.ts generation default to the incorrect version (3.4).
// Explicit imports here resolve the version mismatch.
import markdown from 'remark-parse';
import emoji from 'remark-emoji';
import breaks from 'remark-breaks';
import highlight from '../remark/remark_prismjs';
import * as MarkdownTooltip from '../markdown_tooltip';
import * as MarkdownCheckbox from '../markdown_checkbox';
import { markdownLinkValidator } from '../markdown_link_validator';
export var getDefaultEuiMarkdownParsingPlugins = function getDefaultEuiMarkdownParsingPlugins() {
  var _ref = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      exclude = _ref.exclude;

  var excludeSet = new Set(exclude);
  var parsingPlugins = [[markdown, {}], [highlight, {}], [emoji, {
    emoticon: false
  }], [breaks, {}], [markdownLinkValidator, {}], [MarkdownCheckbox.parser, {}]];
  if (!excludeSet.has('tooltip')) parsingPlugins.push([MarkdownTooltip.parser, {}]);
  return parsingPlugins;
};
export var defaultParsingPlugins = getDefaultEuiMarkdownParsingPlugins();