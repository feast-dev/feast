function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Fragment, createElement } from 'react'; // Importing seemingly unused types from `unified` because the definitions
// are exported for two versions of TypeScript (3.4, 4.0) and implicit
// imports during eui.d.ts generation default to the incorrect version (3.4).
// Explicit imports here resolve the version mismatch.

import all from 'mdast-util-to-hast/lib/all';
import rehype2react from 'rehype-react';
import remark2rehype from 'remark-rehype';
import * as MarkdownTooltip from '../markdown_tooltip';
import * as MarkdownCheckbox from '../markdown_checkbox';
import { FENCED_CLASS } from '../remark/remark_prismjs';
import { EuiLink } from '../../../link';
import { EuiCodeBlock, EuiCode } from '../../../code';
import { EuiHorizontalRule } from '../../../horizontal_rule';
import { jsx as ___EmotionJSX } from "@emotion/react";

var unknownHandler = function unknownHandler(h, node) {
  return h(node, node.type, node, all(h, node));
};

export var getDefaultEuiMarkdownProcessingPlugins = function getDefaultEuiMarkdownProcessingPlugins() {
  var _ref = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      exclude = _ref.exclude;

  var excludeSet = new Set(exclude);
  var plugins = [[remark2rehype, {
    allowDangerousHtml: true,
    unknownHandler: unknownHandler,
    handlers: {} // intentionally empty, allows plugins to extend if they need to

  }], [rehype2react, {
    createElement: createElement,
    Fragment: Fragment,
    components: {
      a: EuiLink,
      code: function code(props) {
        return (// If there are linebreaks use codeblock, otherwise code
          /\r|\n/.exec(props.children) || props.className && props.className.indexOf(FENCED_CLASS) > -1 ? ___EmotionJSX(EuiCodeBlock, _extends({
            fontSize: "m",
            paddingSize: "s",
            isCopyable: true
          }, props)) : ___EmotionJSX(EuiCode, props)
        );
      },
      // When we use block code "fences" the code tag is replaced by the `EuiCodeBlock`.
      // But there's a `pre` tag wrapping all the `EuiCodeBlock`.
      // We want to replace this `pre` tag with a `div` because the `EuiCodeBlock` has its own children `pre` tag.
      pre: function pre(props) {
        return ___EmotionJSX("div", _extends({}, props, {
          className: "euiMarkdownFormat__codeblockWrapper"
        }));
      },
      blockquote: function blockquote(props) {
        return ___EmotionJSX("blockquote", _extends({}, props, {
          className: "euiMarkdownFormat__blockquote"
        }));
      },
      table: function table(props) {
        return ___EmotionJSX("table", _extends({
          className: "euiMarkdownFormat__table"
        }, props));
      },
      hr: function hr(props) {
        return ___EmotionJSX(EuiHorizontalRule, props);
      },
      checkboxPlugin: MarkdownCheckbox.renderer
    }
  }]];
  if (!excludeSet.has('tooltip')) plugins[1][1].components.tooltipPlugin = MarkdownTooltip.renderer;
  return plugins;
};
export var defaultProcessingPlugins = getDefaultEuiMarkdownProcessingPlugins();