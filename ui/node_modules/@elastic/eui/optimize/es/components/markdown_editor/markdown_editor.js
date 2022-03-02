import _extends from "@babel/runtime/helpers/extends";
import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

function _createForOfIteratorHelper(o, allowArrayLike) { var it; if (typeof Symbol === "undefined" || o[Symbol.iterator] == null) { if (Array.isArray(o) || (it = _unsupportedIterableToArray(o)) || allowArrayLike && o && typeof o.length === "number") { if (it) o = it; var i = 0; var F = function F() {}; return { s: F, n: function n() { if (i >= o.length) return { done: true }; return { done: false, value: o[i++] }; }, e: function (_e) { function e(_x) { return _e.apply(this, arguments); } e.toString = function () { return _e.toString(); }; return e; }(function (e) { throw e; }), f: F }; } throw new TypeError("Invalid attempt to iterate non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); } var normalCompletion = true, didErr = false, err; return { s: function s() { it = o[Symbol.iterator](); }, n: function n() { var step = it.next(); normalCompletion = step.done; return step; }, e: function (_e2) { function e(_x2) { return _e2.apply(this, arguments); } e.toString = function () { return _e2.toString(); }; return e; }(function (e) { didErr = true; err = e; }), f: function f() { try { if (!normalCompletion && it.return != null) it.return(); } finally { if (didErr) throw err; } } }; }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { createElement, useEffect, useImperativeHandle, useMemo, useState, useCallback, useRef, forwardRef } from 'react';
import unified from 'unified';
import classNames from 'classnames';
import MarkdownActions, { insertText as _insertText } from './markdown_actions';
import { EuiMarkdownEditorToolbar } from './markdown_editor_toolbar';
import { EuiMarkdownEditorTextArea } from './markdown_editor_text_area';
import { EuiMarkdownFormat } from './markdown_format';
import { EuiMarkdownEditorDropZone } from './markdown_editor_drop_zone';
import { useGeneratedHtmlId } from '../../services/';
import { MODE_EDITING, MODE_VIEWING } from './markdown_modes';
import { EuiModal } from '../modal';
import { EuiMarkdownContext } from './markdown_context';
import * as MarkdownTooltip from './plugins/markdown_tooltip';
import { defaultParsingPlugins, defaultProcessingPlugins, defaultUiPlugins } from './plugins/markdown_default_plugins';
import { EuiResizeObserver } from '../observer/resize_observer';
import { jsx as ___EmotionJSX } from "@emotion/react";

// TODO I wanted to use the useCombinedRefs
// but I can't because it's not allowed to use react hooks
// inside a callback.
var mergeRefs = function mergeRefs() {
  for (var _len = arguments.length, refs = new Array(_len), _key = 0; _key < _len; _key++) {
    refs[_key] = arguments[_key];
  }

  var filteredRefs = refs.filter(Boolean);
  if (!filteredRefs.length) return null;
  if (filteredRefs.length === 0) return filteredRefs[0];
  return function (inst) {
    var _iterator = _createForOfIteratorHelper(filteredRefs),
        _step;

    try {
      for (_iterator.s(); !(_step = _iterator.n()).done;) {
        var ref = _step.value;

        if (typeof ref === 'function') {
          ref(inst);
        } else if (ref) {
          ref.current = inst;
        }
      }
    } catch (err) {
      _iterator.e(err);
    } finally {
      _iterator.f();
    }
  };
};

function isNewLine(char) {
  if (char == null) return true;
  return !!char.match(/[\r\n]/);
}

function padWithNewlinesIfNeeded(textarea, text) {
  var selectionStart = textarea.selectionStart;
  var selectionEnd = textarea.selectionEnd; // block parsing requires two leading new lines and none trailing, but we add an extra trailing line for readability

  var isPrevNewLine = isNewLine(textarea.value[selectionStart - 1]);
  var isPrevPrevNewLine = isNewLine(textarea.value[selectionStart - 2]);
  var isNextNewLine = isNewLine(textarea.value[selectionEnd]); // pad text with newlines as needed

  text = "".concat(isPrevNewLine ? '' : '\n').concat(isPrevPrevNewLine ? '' : '\n').concat(text).concat(isNextNewLine ? '' : '\n');
  return text;
}

export var EuiMarkdownEditor = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var className = _ref.className,
      _editorId = _ref.editorId,
      value = _ref.value,
      _onChange = _ref.onChange,
      _ref$height = _ref.height,
      height = _ref$height === void 0 ? 250 : _ref$height,
      _ref$maxHeight = _ref.maxHeight,
      maxHeight = _ref$maxHeight === void 0 ? 500 : _ref$maxHeight,
      _ref$autoExpandPrevie = _ref.autoExpandPreview,
      autoExpandPreview = _ref$autoExpandPrevie === void 0 ? true : _ref$autoExpandPrevie,
      _ref$parsingPluginLis = _ref.parsingPluginList,
      parsingPluginList = _ref$parsingPluginLis === void 0 ? defaultParsingPlugins : _ref$parsingPluginLis,
      _ref$processingPlugin = _ref.processingPluginList,
      processingPluginList = _ref$processingPlugin === void 0 ? defaultProcessingPlugins : _ref$processingPlugin,
      _ref$uiPlugins = _ref.uiPlugins,
      uiPlugins = _ref$uiPlugins === void 0 ? defaultUiPlugins : _ref$uiPlugins,
      onParse = _ref.onParse,
      _ref$errors = _ref.errors,
      errors = _ref$errors === void 0 ? [] : _ref$errors,
      ariaLabel = _ref['aria-label'],
      ariaLabelledBy = _ref['aria-labelledby'],
      ariaDescribedBy = _ref['aria-describedby'],
      _ref$initialViewMode = _ref.initialViewMode,
      initialViewMode = _ref$initialViewMode === void 0 ? MODE_EDITING : _ref$initialViewMode,
      _ref$dropHandlers = _ref.dropHandlers,
      dropHandlers = _ref$dropHandlers === void 0 ? [] : _ref$dropHandlers,
      markdownFormatProps = _ref.markdownFormatProps,
      placeholder = _ref.placeholder,
      rest = _objectWithoutProperties(_ref, ["className", "editorId", "value", "onChange", "height", "maxHeight", "autoExpandPreview", "parsingPluginList", "processingPluginList", "uiPlugins", "onParse", "errors", "aria-label", "aria-labelledby", "aria-describedby", "initialViewMode", "dropHandlers", "markdownFormatProps", "placeholder"]);

  var _useState = useState(initialViewMode),
      _useState2 = _slicedToArray(_useState, 2),
      viewMode = _useState2[0],
      setViewMode = _useState2[1];

  var editorId = useGeneratedHtmlId({
    conditionalId: _editorId
  });

  var _useState3 = useState(undefined),
      _useState4 = _slicedToArray(_useState3, 2),
      pluginEditorPlugin = _useState4[0],
      setPluginEditorPlugin = _useState4[1];

  var toolbarPlugins = _toConsumableArray(uiPlugins); // @ts-ignore __originatedFromEui is a custom property


  if (!uiPlugins.__originatedFromEui) {
    toolbarPlugins.unshift(MarkdownTooltip.plugin);
    console.warn('Deprecation warning: uiPlugins passed to EuiMarkdownEditor does not include the tooltip plugin, which has been added for you. This automatic inclusion has been deprecated and will be removed in the future, see https://github.com/elastic/eui/pull/4383');
  }

  var markdownActions = useMemo(function () {
    return new MarkdownActions(editorId, toolbarPlugins);
  }, // toolbarPlugins _is_ accounted for
  // eslint-disable-next-line react-hooks/exhaustive-deps
  [editorId, toolbarPlugins.map(function (_ref2) {
    var name = _ref2.name;
    return name;
  }).join(',')]);
  var parser = useMemo(function () {
    var Compiler = function Compiler(tree) {
      return tree;
    };

    function identityCompiler() {
      this.Compiler = Compiler;
    }

    return unified().use(parsingPluginList).use(identityCompiler);
  }, [parsingPluginList]);

  var _useMemo = useMemo(function () {
    try {
      var _parsed = parser.processSync(value);

      return [_parsed, null];
    } catch (e) {
      return [null, e];
    }
  }, [parser, value]),
      _useMemo2 = _slicedToArray(_useMemo, 2),
      parsed = _useMemo2[0],
      parseError = _useMemo2[1];

  var isPreviewing = viewMode === MODE_VIEWING;
  var isEditing = viewMode === MODE_EDITING;
  var replaceNode = useCallback(function (position, next) {
    var leading = value.substr(0, position.start.offset);
    var trailing = value.substr(position.end.offset);

    _onChange("".concat(leading).concat(next).concat(trailing));
  }, [value, _onChange]);
  var contextValue = useMemo(function () {
    return {
      openPluginEditor: function openPluginEditor(plugin) {
        return setPluginEditorPlugin(function () {
          return plugin;
        });
      },
      replaceNode: replaceNode
    };
  }, [replaceNode]);

  var _useState5 = useState(),
      _useState6 = _slicedToArray(_useState5, 2),
      selectedNode = _useState6[0],
      setSelectedNode = _useState6[1];

  var textareaRef = useRef(null);
  useEffect(function () {
    if (textareaRef == null) return;
    if (parsed == null) return;

    var getCursorNode = function getCursorNode() {
      var _parsed$result;

      var _ref3 = textareaRef.current,
          selectionStart = _ref3.selectionStart;
      var node = (_parsed$result = parsed.result) !== null && _parsed$result !== void 0 ? _parsed$result : parsed.contents;

      outer: while (true) {
        if (node.children) {
          for (var i = 0; i < node.children.length; i++) {
            var child = node.children[i];

            if (child.position.start.offset < selectionStart && selectionStart < child.position.end.offset) {
              if (child.type === 'text') break outer; // don't dive into `text` nodes

              node = child;
              continue outer;
            }
          }
        }

        break;
      }

      setSelectedNode(node);
    };

    var textarea = textareaRef.current;
    textarea.addEventListener('keyup', getCursorNode);
    textarea.addEventListener('mouseup', getCursorNode);
    return function () {
      textarea.removeEventListener('keyup', getCursorNode);
      textarea.removeEventListener('mouseup', getCursorNode);
    };
  }, [parsed]);
  useEffect(function () {
    if (onParse) {
      var _parsed$result2;

      var messages = parsed ? parsed.messages : [];
      var ast = parsed ? (_parsed$result2 = parsed.result) !== null && _parsed$result2 !== void 0 ? _parsed$result2 : parsed.contents : null;
      onParse(parseError, {
        messages: messages,
        ast: ast
      });
    }
  }, [onParse, parsed, parseError]);
  useImperativeHandle(ref, function () {
    return {
      textarea: textareaRef.current,
      replaceNode: replaceNode
    };
  }, [replaceNode]);
  var textarea = textareaRef.current;
  var previewRef = useRef(null);
  var editorToolbarRef = useRef(null);

  var _React$useState = React.useState(false),
      _React$useState2 = _slicedToArray(_React$useState, 2),
      hasUnacceptedItems = _React$useState2[0],
      setHasUnacceptedItems = _React$useState2[1];

  var _useState7 = useState(height),
      _useState8 = _slicedToArray(_useState7, 2),
      currentHeight = _useState8[0],
      setCurrentHeight = _useState8[1];

  var _useState9 = useState(0),
      _useState10 = _slicedToArray(_useState9, 2),
      editorFooterHeight = _useState10[0],
      setEditorFooterHeight = _useState10[1];

  var _useState11 = useState(0),
      _useState12 = _slicedToArray(_useState11, 2),
      editorToolbarHeight = _useState12[0],
      setEditorToolbarHeight = _useState12[1];

  var classes = classNames('euiMarkdownEditor', {
    'euiMarkdownEditor--fullHeight': height === 'full'
  }, {
    'euiMarkdownEditor--isPreviewing': isPreviewing
  }, className);

  var onResize = function onResize() {
    if (textarea && isEditing && height !== 'full') {
      var resizedTextareaHeight = textarea.offsetHeight + editorFooterHeight;
      setCurrentHeight(resizedTextareaHeight);
    }
  };

  useEffect(function () {
    setEditorToolbarHeight(editorToolbarRef.current.offsetHeight);
  }, [setEditorToolbarHeight]);
  useEffect(function () {
    if (isPreviewing && autoExpandPreview && height !== 'full') {
      if (previewRef.current.scrollHeight > currentHeight) {
        // scrollHeight does not include the border or margin
        // so we ask for the computed value for those,
        // which is always in pixels because getComputedValue
        // returns the resolved values
        var elementComputedStyle = window.getComputedStyle(previewRef.current);
        var borderWidth = parseFloat(elementComputedStyle.borderTopWidth) + parseFloat(elementComputedStyle.borderBottomWidth);
        var marginWidth = parseFloat(elementComputedStyle.marginTop) + parseFloat(elementComputedStyle.marginBottom); // then add an extra pixel for safety and because the scrollHeight value is rounded

        var extraHeight = borderWidth + marginWidth + 1;
        setCurrentHeight(previewRef.current.scrollHeight + extraHeight);
      }
    }
  }, [currentHeight, isPreviewing, height, autoExpandPreview]);
  var previewHeight = height === 'full' ? "calc(100% - ".concat(editorFooterHeight, "px)") : currentHeight;
  var textAreaHeight = height === 'full' ? '100%' : "calc(".concat(height - editorFooterHeight, "px)");
  var textAreaMaxHeight = height !== 'full' ? "".concat(maxHeight - editorFooterHeight, "px") : ''; // safari needs this calc when the height is set to full

  var editorToggleContainerHeight = "calc(100% - ".concat(editorToolbarHeight, "px)");
  return ___EmotionJSX(EuiMarkdownContext.Provider, {
    value: contextValue
  }, ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX(EuiMarkdownEditorToolbar, {
    ref: editorToolbarRef,
    selectedNode: selectedNode,
    markdownActions: markdownActions,
    onClickPreview: function onClickPreview() {
      return setViewMode(isPreviewing ? MODE_EDITING : MODE_VIEWING);
    },
    viewMode: viewMode,
    uiPlugins: toolbarPlugins
  }), isPreviewing && ___EmotionJSX("div", {
    ref: previewRef,
    className: "euiMarkdownEditorPreview",
    style: {
      height: previewHeight
    }
  }, ___EmotionJSX(EuiMarkdownFormat, _extends({
    parsingPluginList: parsingPluginList,
    processingPluginList: processingPluginList
  }, markdownFormatProps), value)), ___EmotionJSX("div", {
    className: "euiMarkdownEditor__toggleContainer",
    style: {
      height: editorToggleContainerHeight
    }
  }, ___EmotionJSX(EuiMarkdownEditorDropZone, {
    setEditorFooterHeight: setEditorFooterHeight,
    isEditing: isEditing,
    dropHandlers: dropHandlers,
    insertText: function insertText(text, config) {
      if (config.block) {
        text = padWithNewlinesIfNeeded(textareaRef.current, text);
      }

      var originalSelectionStart = textareaRef.current.selectionStart;
      var newSelectionPoint = originalSelectionStart + text.length;

      _insertText(textareaRef.current, {
        text: text,
        selectionStart: newSelectionPoint,
        selectionEnd: newSelectionPoint
      });
    },
    uiPlugins: toolbarPlugins,
    errors: errors,
    hasUnacceptedItems: hasUnacceptedItems,
    setHasUnacceptedItems: setHasUnacceptedItems
  }, ___EmotionJSX(EuiResizeObserver, {
    onResize: onResize
  }, function (resizeRef) {
    return ___EmotionJSX(EuiMarkdownEditorTextArea, _extends({
      height: textAreaHeight,
      maxHeight: textAreaMaxHeight,
      ref: mergeRefs(textareaRef, resizeRef),
      id: editorId,
      onChange: function onChange(e) {
        return _onChange(e.target.value);
      },
      value: value,
      onFocus: function onFocus() {
        return setHasUnacceptedItems(false);
      },
      placeholder: placeholder
    }, {
      'aria-label': ariaLabel,
      'aria-labelledby': ariaLabelledBy,
      'aria-describedby': ariaDescribedBy
    }));
  })), pluginEditorPlugin && ___EmotionJSX(EuiModal, {
    onClose: function onClose() {
      return setPluginEditorPlugin(undefined);
    }
  }, /*#__PURE__*/createElement(pluginEditorPlugin.editor, {
    node: selectedNode && selectedNode.type === pluginEditorPlugin.name ? selectedNode : null,
    onCancel: function onCancel() {
      return setPluginEditorPlugin(undefined);
    },
    onSave: function onSave(markdown, config) {
      if (selectedNode && selectedNode.type === pluginEditorPlugin.name) {
        // modifying an existing node
        textareaRef.current.setSelectionRange(selectedNode.position.start.offset, selectedNode.position.end.offset);
      } else {
        // creating a new node
        if (config.block) {
          // inject newlines if needed
          markdown = padWithNewlinesIfNeeded(textareaRef.current, markdown);
        }
      }

      _insertText(textareaRef.current, {
        text: markdown,
        selectionStart: undefined,
        selectionEnd: undefined
      });

      setPluginEditorPlugin(undefined);
    }
  })))));
});
EuiMarkdownEditor.displayName = 'EuiMarkdownEditor';