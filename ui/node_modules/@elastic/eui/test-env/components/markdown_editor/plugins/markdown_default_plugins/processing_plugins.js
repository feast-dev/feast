"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.defaultProcessingPlugins = exports.getDefaultEuiMarkdownProcessingPlugins = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _react = _interopRequireWildcard(require("react"));

var _all = _interopRequireDefault(require("mdast-util-to-hast/lib/all"));

var _rehypeReact = _interopRequireDefault(require("rehype-react"));

var _remarkRehype = _interopRequireDefault(require("remark-rehype"));

var MarkdownTooltip = _interopRequireWildcard(require("../markdown_tooltip"));

var MarkdownCheckbox = _interopRequireWildcard(require("../markdown_checkbox"));

var _remark_prismjs = require("../remark/remark_prismjs");

var _link = require("../../../link");

var _code = require("../../../code");

var _horizontal_rule = require("../../../horizontal_rule");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var unknownHandler = function unknownHandler(h, node) {
  return h(node, node.type, node, (0, _all.default)(h, node));
};

var getDefaultEuiMarkdownProcessingPlugins = function getDefaultEuiMarkdownProcessingPlugins() {
  var _ref = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      exclude = _ref.exclude;

  var excludeSet = new Set(exclude);
  var plugins = [[_remarkRehype.default, {
    allowDangerousHtml: true,
    unknownHandler: unknownHandler,
    handlers: {} // intentionally empty, allows plugins to extend if they need to

  }], [_rehypeReact.default, {
    createElement: _react.createElement,
    Fragment: _react.Fragment,
    components: {
      a: _link.EuiLink,
      code: function code(props) {
        return (// If there are linebreaks use codeblock, otherwise code
          /\r|\n/.exec(props.children) || props.className && props.className.indexOf(_remark_prismjs.FENCED_CLASS) > -1 ? (0, _react2.jsx)(_code.EuiCodeBlock, (0, _extends2.default)({
            fontSize: "m",
            paddingSize: "s",
            isCopyable: true
          }, props)) : (0, _react2.jsx)(_code.EuiCode, props)
        );
      },
      // When we use block code "fences" the code tag is replaced by the `EuiCodeBlock`.
      // But there's a `pre` tag wrapping all the `EuiCodeBlock`.
      // We want to replace this `pre` tag with a `div` because the `EuiCodeBlock` has its own children `pre` tag.
      pre: function pre(props) {
        return (0, _react2.jsx)("div", (0, _extends2.default)({}, props, {
          className: "euiMarkdownFormat__codeblockWrapper"
        }));
      },
      blockquote: function blockquote(props) {
        return (0, _react2.jsx)("blockquote", (0, _extends2.default)({}, props, {
          className: "euiMarkdownFormat__blockquote"
        }));
      },
      table: function table(props) {
        return (0, _react2.jsx)("table", (0, _extends2.default)({
          className: "euiMarkdownFormat__table"
        }, props));
      },
      hr: function hr(props) {
        return (0, _react2.jsx)(_horizontal_rule.EuiHorizontalRule, props);
      },
      checkboxPlugin: MarkdownCheckbox.renderer
    }
  }]];
  if (!excludeSet.has('tooltip')) plugins[1][1].components.tooltipPlugin = MarkdownTooltip.renderer;
  return plugins;
};

exports.getDefaultEuiMarkdownProcessingPlugins = getDefaultEuiMarkdownProcessingPlugins;
var defaultProcessingPlugins = getDefaultEuiMarkdownProcessingPlugins();
exports.defaultProcessingPlugins = defaultProcessingPlugins;