"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.insertText = insertText;
exports.default = void 0;

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _markdown_types = require("./markdown_types");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/**
 * Class for applying styles to a text editor. Accepts the HTML ID for the textarea
 * desired, and exposes the `.do(ACTION)` method for manipulating the text.
 *
 * @class MarkdownActions
 * @param {string} editorID
 */
var MarkdownActions = /*#__PURE__*/function () {
  function MarkdownActions(editorID, uiPlugins) {
    (0, _classCallCheck2.default)(this, MarkdownActions);
    this.editorID = editorID;
    (0, _defineProperty2.default)(this, "styles", void 0);

    /**
     * This object is in the format:
     * [nameOfAction]: {[styles to apply]}
     */
    this.styles = _objectSpread(_objectSpread({}, uiPlugins.reduce(function (mappedPlugins, plugin) {
      mappedPlugins[plugin.name] = plugin;
      return mappedPlugins;
    }, {})), {}, {
      mdBold: {
        name: 'mdBold',
        button: {
          label: '',
          iconType: ''
        },
        formatting: {
          prefix: '**',
          suffix: '**',
          trimFirst: true
        }
      },
      mdItalic: {
        name: 'mdItalic',
        button: {
          label: '',
          iconType: ''
        },
        formatting: {
          prefix: '_',
          suffix: '_',
          trimFirst: true
        }
      },
      mdQuote: {
        name: 'mdQuote',
        button: {
          label: '',
          iconType: ''
        },
        formatting: {
          prefix: '> ',
          multiline: true,
          surroundWithNewlines: true
        }
      },
      mdCode: {
        name: 'mdCode',
        button: {
          label: '',
          iconType: ''
        },
        formatting: {
          prefix: '`',
          suffix: '`',
          blockPrefix: '```',
          blockSuffix: '```'
        }
      },
      mdLink: {
        name: 'mdLink',
        button: {
          label: '',
          iconType: ''
        },
        formatting: {
          prefix: '[',
          suffix: '](url)',
          replaceNext: 'url',
          scanFor: 'https?://'
        }
      },
      mdUl: {
        name: 'mdUl',
        button: {
          label: '',
          iconType: ''
        },
        formatting: {
          prefix: '- ',
          multiline: true,
          surroundWithNewlines: true
        }
      },
      mdOl: {
        name: 'mdOl',
        button: {
          label: '',
          iconType: ''
        },
        formatting: {
          prefix: '1. ',
          multiline: true,
          orderedList: true
        }
      },
      mdTl: {
        name: 'mdTl',
        button: {
          label: '',
          iconType: ''
        },
        formatting: {
          prefix: '- [ ] ',
          multiline: true,
          surroundWithNewlines: true
        }
      }
    });
  }
  /**
   * .do() accepts a string and retrieves the correlating style object (defined in the
   * constructor). It passes this to applyStyle() that does the text manipulation.
   *
   * @param {string} pluginName
   * @memberof MarkdownActions
   */


  (0, _createClass2.default)(MarkdownActions, [{
    key: "do",
    value: function _do(pluginName) {
      var plugin = this.styles[pluginName];

      if ((0, _markdown_types.isPluginWithImmediateFormatting)(plugin)) {
        this.applyStyle(plugin.formatting);
        return true;
      } else {
        return plugin;
      }
    }
    /**
     * Sets the default styling object and then superimposes the changes to make on top of
     * it. Calls the `styleSelectedText` helper function that does the heavy lifting.
     * Adapted from https://github.com/github/markdown-toolbar-element/blob/main/src/index.ts
     *
     * @param {object} incomingStyle
     * @memberof MarkdownActions
     */

  }, {
    key: "applyStyle",
    value: function applyStyle(incomingStyle) {
      var defaults = {
        prefix: '',
        suffix: '',
        blockPrefix: '',
        blockSuffix: '',
        multiline: false,
        replaceNext: '',
        prefixSpace: false,
        scanFor: '',
        surroundWithNewlines: false,
        orderedList: false,
        trimFirst: false
      };

      var outgoingStyle = _objectSpread(_objectSpread({}, defaults), incomingStyle);

      var editor = document.getElementById(this.editorID);

      if (editor) {
        editor.focus();
        styleSelectedText(editor, outgoingStyle);
      }
    }
  }]);
  return MarkdownActions;
}();
/**
 * The following helper functions and types were copied from the GitHub Markdown Toolbar
 * Element project. The project is MIT-licensed. See it here:
 * https://github.com/github/markdown-toolbar-element
 */


function isMultipleLines(string) {
  return string.trim().split('\n').length > 1;
}

function repeat(string, n) {
  return Array(n + 1).join(string);
}

function wordSelectionStart(text, i) {
  var index = i;

  while (text[index] && text[index - 1] != null && !text[index - 1].match(/\s/)) {
    index--;
  }

  return index;
}

function wordSelectionEnd(text, i, multiline) {
  var index = i;
  var breakpoint = multiline ? /\n/ : /\s/;

  while (text[index] && !text[index].match(breakpoint)) {
    index++;
  }

  return index;
}

var MAX_TRIES = 10;
var TRY_TIMEOUT = 10;
/*ms*/
// modified from https://github.com/github/markdown-toolbar-element/blob/main/src/index.ts

function insertText(textarea, _ref) {
  var text = _ref.text,
      selectionStart = _ref.selectionStart,
      selectionEnd = _ref.selectionEnd;
  var originalSelectionStart = textarea.selectionStart;
  var before = textarea.value.slice(0, originalSelectionStart);
  var after = textarea.value.slice(textarea.selectionEnd); // configuration modal/dialog will continue intercepting focus in Safari
  // need to wait until the textarea can receive focus

  var tries = 0;

  var insertText = function insertText() {
    var insertResult = document.execCommand('insertText', false, text);

    if (insertResult === false) {
      /**
       * Fallback for Firefox; this kills undo/redo but at least updates the value
       *
       * Note that we're using the native HTMLTextAreaElement.set() method to play nicely with
       * React's synthetic event system.
       * https://hustle.bizongo.in/simulate-react-on-change-on-controlled-components-baa336920e04
       */
      var inputEvent = new Event('input', {
        bubbles: true
      });
      var nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value').set;
      nativeInputValueSetter.call(textarea, before + text + after);
      textarea.dispatchEvent(inputEvent);
    }

    if (selectionStart != null && selectionEnd != null) {
      textarea.setSelectionRange(selectionStart, selectionEnd);
    } else {
      textarea.setSelectionRange(originalSelectionStart, textarea.selectionEnd);
    }
  };

  var focusTextarea = function focusTextarea() {
    textarea.focus();

    if (document.activeElement === textarea) {
      insertText();
    } else if (++tries === MAX_TRIES) {
      insertText();
    } else {
      setTimeout(focusTextarea, TRY_TIMEOUT);
    }
  };

  focusTextarea();
} // from https://github.com/github/markdown-toolbar-element/blob/main/src/index.ts


function styleSelectedText(textarea, styleArgs) {
  var text = textarea.value.slice(textarea.selectionStart, textarea.selectionEnd);
  var result;

  if (styleArgs.orderedList) {
    result = orderedList(textarea);
  } else if (styleArgs.multiline && isMultipleLines(text)) {
    result = multilineStyle(textarea, styleArgs);
  } else {
    result = blockStyle(textarea, styleArgs);
  }

  insertText(textarea, result);
}

function expandSelectedText(textarea, prefixToUse, suffixToUse) {
  var multiline = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : false;

  if (textarea.selectionStart === textarea.selectionEnd) {
    textarea.selectionStart = wordSelectionStart(textarea.value, textarea.selectionStart);
    textarea.selectionEnd = wordSelectionEnd(textarea.value, textarea.selectionEnd, multiline);
  } else {
    var expandedSelectionStart = textarea.selectionStart - prefixToUse.length;
    var expandedSelectionEnd = textarea.selectionEnd + suffixToUse.length;
    var beginsWithPrefix = textarea.value.slice(expandedSelectionStart, textarea.selectionStart) === prefixToUse;
    var endsWithSuffix = textarea.value.slice(textarea.selectionEnd, expandedSelectionEnd) === suffixToUse;

    if (beginsWithPrefix && endsWithSuffix) {
      textarea.selectionStart = expandedSelectionStart;
      textarea.selectionEnd = expandedSelectionEnd;
    }
  }

  return textarea.value.slice(textarea.selectionStart, textarea.selectionEnd);
}

function newlinesToSurroundSelectedText(textarea) {
  var beforeSelection = textarea.value.slice(0, textarea.selectionStart);
  var afterSelection = textarea.value.slice(textarea.selectionEnd);
  var breaksBefore = beforeSelection.match(/\n*$/);
  var breaksAfter = afterSelection.match(/^\n*/);
  var newlinesBeforeSelection = breaksBefore ? breaksBefore[0].length : 0;
  var newlinesAfterSelection = breaksAfter ? breaksAfter[0].length : 0;
  var newlinesToAppend;
  var newlinesToPrepend;

  if (beforeSelection.match(/\S/) && newlinesBeforeSelection < 2) {
    newlinesToAppend = repeat('\n', 2 - newlinesBeforeSelection);
  }

  if (afterSelection.match(/\S/) && newlinesAfterSelection < 2) {
    newlinesToPrepend = repeat('\n', 2 - newlinesAfterSelection);
  }

  if (newlinesToAppend == null) {
    newlinesToAppend = '';
  }

  if (newlinesToPrepend == null) {
    newlinesToPrepend = '';
  }

  return {
    newlinesToAppend: newlinesToAppend,
    newlinesToPrepend: newlinesToPrepend
  };
}

function blockStyle(textarea, arg) {
  var newlinesToAppend;
  var newlinesToPrepend;
  var prefix = arg.prefix,
      suffix = arg.suffix,
      blockPrefix = arg.blockPrefix,
      blockSuffix = arg.blockSuffix,
      replaceNext = arg.replaceNext,
      prefixSpace = arg.prefixSpace,
      scanFor = arg.scanFor,
      surroundWithNewlines = arg.surroundWithNewlines;
  var originalSelectionStart = textarea.selectionStart;
  var originalSelectionEnd = textarea.selectionEnd;
  var selectedText = textarea.value.slice(textarea.selectionStart, textarea.selectionEnd);
  var prefixToUse = isMultipleLines(selectedText) && blockPrefix.length > 0 ? "".concat(blockPrefix, "\n") : prefix;
  var suffixToUse = isMultipleLines(selectedText) && blockSuffix.length > 0 ? "\n".concat(blockSuffix) : suffix;

  if (prefixSpace) {
    var beforeSelection = textarea.value[textarea.selectionStart - 1];

    if (textarea.selectionStart !== 0 && beforeSelection != null && !beforeSelection.match(/\s/)) {
      prefixToUse = " ".concat(prefixToUse);
    }
  }

  selectedText = expandSelectedText(textarea, prefixToUse, suffixToUse, arg.multiline);
  var selectionStart = textarea.selectionStart;
  var selectionEnd = textarea.selectionEnd;
  var hasReplaceNext = replaceNext.length > 0 && suffixToUse.indexOf(replaceNext) > -1 && selectedText.length > 0;

  if (surroundWithNewlines) {
    var ref = newlinesToSurroundSelectedText(textarea);
    newlinesToAppend = ref.newlinesToAppend;
    newlinesToPrepend = ref.newlinesToPrepend;
    prefixToUse = newlinesToAppend + prefix;
    suffixToUse += newlinesToPrepend;
  }

  if (selectedText.startsWith(prefixToUse) && selectedText.endsWith(suffixToUse)) {
    var replacementText = selectedText.slice(prefixToUse.length, selectedText.length - suffixToUse.length);

    if (originalSelectionStart === originalSelectionEnd) {
      var position = originalSelectionStart - prefixToUse.length;
      position = Math.max(position, selectionStart);
      position = Math.min(position, selectionStart + replacementText.length);
      selectionStart = selectionEnd = position;
    } else {
      selectionEnd = selectionStart + replacementText.length;
    }

    return {
      text: replacementText,
      selectionStart: selectionStart,
      selectionEnd: selectionEnd
    };
  } else if (!hasReplaceNext) {
    var _replacementText = prefixToUse + selectedText + suffixToUse;

    selectionStart = originalSelectionStart + prefixToUse.length;
    selectionEnd = originalSelectionEnd + prefixToUse.length;
    var whitespaceEdges = selectedText.match(/^\s*|\s*$/g);

    if (arg.trimFirst && whitespaceEdges) {
      var leadingWhitespace = whitespaceEdges[0] || '';
      var trailingWhitespace = whitespaceEdges[1] || '';
      _replacementText = leadingWhitespace + prefixToUse + selectedText.trim() + suffixToUse + trailingWhitespace;
      selectionStart += leadingWhitespace.length;
      selectionEnd -= trailingWhitespace.length;
    }

    return {
      text: _replacementText,
      selectionStart: selectionStart,
      selectionEnd: selectionEnd
    };
  } else if (scanFor.length > 0 && selectedText.match(scanFor)) {
    suffixToUse = suffixToUse.replace(replaceNext, selectedText);

    var _replacementText2 = prefixToUse + suffixToUse;

    selectionStart = selectionEnd = selectionStart + prefixToUse.length;
    return {
      text: _replacementText2,
      selectionStart: selectionStart,
      selectionEnd: selectionEnd
    };
  } else {
    var _replacementText3 = prefixToUse + selectedText + suffixToUse;

    selectionStart = selectionStart + prefixToUse.length + selectedText.length + suffixToUse.indexOf(replaceNext);
    selectionEnd = selectionStart + replaceNext.length;
    return {
      text: _replacementText3,
      selectionStart: selectionStart,
      selectionEnd: selectionEnd
    };
  }
}

function multilineStyle(textarea, arg) {
  var prefix = arg.prefix,
      suffix = arg.suffix,
      surroundWithNewlines = arg.surroundWithNewlines;
  var text = textarea.value.slice(textarea.selectionStart, textarea.selectionEnd);
  var selectionStart = textarea.selectionStart;
  var selectionEnd = textarea.selectionEnd;
  var lines = text.split('\n');
  var undoStyle = lines.every(function (line) {
    return line.startsWith(prefix) && line.endsWith(suffix);
  });

  if (undoStyle) {
    text = lines.map(function (line) {
      return line.slice(prefix.length, line.length - suffix.length);
    }).join('\n');
    selectionEnd = selectionStart + text.length;
  } else {
    text = lines.map(function (line) {
      return prefix + line + suffix;
    }).join('\n');

    if (surroundWithNewlines) {
      var _newlinesToSurroundSe = newlinesToSurroundSelectedText(textarea),
          newlinesToAppend = _newlinesToSurroundSe.newlinesToAppend,
          newlinesToPrepend = _newlinesToSurroundSe.newlinesToPrepend;

      selectionStart += newlinesToAppend.length;
      selectionEnd = selectionStart + text.length;
      text = newlinesToAppend + text + newlinesToPrepend;
    }
  }

  return {
    text: text,
    selectionStart: selectionStart,
    selectionEnd: selectionEnd
  };
}

function orderedList(textarea) {
  var orderedListRegex = /^\d+\.\s+/;
  var noInitialSelection = textarea.selectionStart === textarea.selectionEnd;
  var selectionEnd;
  var selectionStart;
  var text = textarea.value.slice(textarea.selectionStart, textarea.selectionEnd);
  var textToUnstyle = text;
  var lines = text.split('\n');
  var startOfLine;
  var endOfLine;

  if (noInitialSelection) {
    var linesBefore = textarea.value.slice(0, textarea.selectionStart).split(/\n/);
    startOfLine = textarea.selectionStart - linesBefore[linesBefore.length - 1].length;
    endOfLine = wordSelectionEnd(textarea.value, textarea.selectionStart, true);
    textToUnstyle = textarea.value.slice(startOfLine, endOfLine);
  }

  var linesToUnstyle = textToUnstyle.split('\n');
  var undoStyling = linesToUnstyle.every(function (line) {
    return orderedListRegex.test(line);
  });

  if (undoStyling) {
    lines = linesToUnstyle.map(function (line) {
      return line.replace(orderedListRegex, '');
    });
    text = lines.join('\n');

    if (noInitialSelection && startOfLine && endOfLine) {
      var lengthDiff = linesToUnstyle[0].length - lines[0].length;
      selectionStart = selectionEnd = textarea.selectionStart - lengthDiff;
      textarea.selectionStart = startOfLine;
      textarea.selectionEnd = endOfLine;
    }
  } else {
    lines = function () {
      var i;
      var len;
      var index;
      var results = [];

      for (index = i = 0, len = lines.length; i < len; index = ++i) {
        var line = lines[index];
        results.push("".concat(index + 1, ". ").concat(line));
      }

      return results;
    }();

    text = lines.join('\n');

    var _newlinesToSurroundSe2 = newlinesToSurroundSelectedText(textarea),
        newlinesToAppend = _newlinesToSurroundSe2.newlinesToAppend,
        newlinesToPrepend = _newlinesToSurroundSe2.newlinesToPrepend;

    selectionStart = textarea.selectionStart + newlinesToAppend.length;
    selectionEnd = selectionStart + text.length;
    if (noInitialSelection) selectionStart = selectionEnd;
    text = newlinesToAppend + text + newlinesToPrepend;
  }

  return {
    text: text,
    selectionStart: selectionStart,
    selectionEnd: selectionEnd
  };
}

var _default = MarkdownActions;
exports.default = _default;