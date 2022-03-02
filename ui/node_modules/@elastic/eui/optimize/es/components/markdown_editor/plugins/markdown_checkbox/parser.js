import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
export var CheckboxParser = function CheckboxParser() {
  var Parser = this.Parser;
  var tokenizers = Parser.prototype.blockTokenizers;
  var methods = Parser.prototype.blockMethods;

  var tokenizeCheckbox = function tokenizeCheckbox(eat, value, silent) {
    /**
     * optional leading whitespace & single (dash or asterisk) mix
     * square brackets, optionally containing whitespace and `x`
     * optional whitespace
     * remainder of the line is consumed as the textbox label
     */
    var checkboxMatch = value.match(/^(\s*[-*]\s*)?\[([\sx]*)\](.+)/);
    if (checkboxMatch == null) return false;

    if (silent) {
      return true;
    }

    var _checkboxMatch = _slicedToArray(checkboxMatch, 4),
        match = _checkboxMatch[0],
        _checkboxMatch$ = _checkboxMatch[1],
        lead = _checkboxMatch$ === void 0 ? '' : _checkboxMatch$,
        checkboxStatus = _checkboxMatch[2],
        text = _checkboxMatch[3];

    var isChecked = checkboxStatus.indexOf('x') !== -1;
    var now = eat.now();
    var offset = match.length - text.length;
    now.column += offset;
    now.offset += offset;
    var children = this.tokenizeInline(text, now);
    return eat(match)({
      type: 'checkboxPlugin',
      lead: lead,
      label: text,
      isChecked: isChecked,
      children: children
    });
  };

  tokenizers.checkbox = tokenizeCheckbox;
  methods.splice(methods.indexOf('list'), 0, 'checkbox'); // Run it just before default `list` plugin to inject our own idea of checkboxes.
};