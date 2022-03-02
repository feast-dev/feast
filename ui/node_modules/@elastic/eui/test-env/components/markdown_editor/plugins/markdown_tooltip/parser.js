"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.TooltipParser = void 0;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var TooltipParser = function TooltipParser() {
  var Parser = this.Parser;
  var tokenizers = Parser.prototype.inlineTokenizers;
  var methods = Parser.prototype.inlineMethods;

  var tokenizeTooltip = function tokenizeTooltip(eat, value, silent) {
    if (value.startsWith('!{tooltip') === false) return false;
    var nextChar = value[9];
    if (nextChar !== '[') return false; // this isn't actually a tooltip

    var index = 9;

    function readArg(open, close) {
      if (value[index] !== open) throw 'Expected left bracket';
      index++;
      var body = '';
      var openBrackets = 0;

      for (; index < value.length; index++) {
        var char = value[index];

        if (char === close && openBrackets === 0) {
          index++;
          return body;
        } else if (char === close) {
          openBrackets--;
        } else if (char === open) {
          openBrackets++;
        }

        body += char;
      }

      return '';
    }

    var tooltipAnchor = readArg('[', ']');
    var tooltipText = readArg('(', ')');
    var now = eat.now();

    if (!tooltipAnchor) {
      this.file.info('No tooltip anchor found', {
        line: now.line,
        column: now.column + 10
      });
    }

    if (!tooltipText) {
      this.file.info('No tooltip text found', {
        line: now.line,
        column: now.column + 12 + tooltipAnchor.length
      });
    }

    if (!tooltipText || !tooltipAnchor) return false;

    if (silent) {
      return true;
    }

    now.column += 10;
    now.offset += 10;
    var children = this.tokenizeInline(tooltipAnchor, now);
    return eat("!{tooltip[".concat(tooltipAnchor, "](").concat(tooltipText, ")}"))({
      type: 'tooltipPlugin',
      content: tooltipText,
      children: children
    });
  };

  tokenizeTooltip.notInLink = true;

  tokenizeTooltip.locator = function (value, fromIndex) {
    return value.indexOf('!{tooltip', fromIndex);
  };

  tokenizers.tooltip = tokenizeTooltip;
  methods.splice(methods.indexOf('text'), 0, 'tooltip');
};

exports.TooltipParser = TooltipParser;