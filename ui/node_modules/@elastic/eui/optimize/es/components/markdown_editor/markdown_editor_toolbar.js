import _extends from "@babel/runtime/helpers/extends";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useContext, forwardRef } from 'react';
import { EuiButtonEmpty, EuiButtonIcon } from '../button';
import { EuiI18n } from '../i18n';
import { EuiToolTip } from '../tool_tip';
import { MODE_VIEWING } from './markdown_modes';
import { EuiMarkdownContext } from './markdown_context';
// @ts-ignore a react svg
import MarkdownCheckmarkIcon from './icons/markdown_checkmark';
import { jsx as ___EmotionJSX } from "@emotion/react";
var boldItalicButtons = [{
  id: 'mdBold',
  label: 'Bold',
  name: 'bold',
  iconType: 'editorBold'
}, {
  id: 'mdItalic',
  label: 'Italic',
  name: 'italic',
  iconType: 'editorItalic'
}];
var listButtons = [{
  id: 'mdUl',
  label: 'Unordered list',
  name: 'ul',
  iconType: 'editorUnorderedList'
}, {
  id: 'mdOl',
  label: 'Ordered list',
  name: 'ol',
  iconType: 'editorOrderedList'
}, {
  id: 'mdTl',
  label: 'Task list',
  name: 'tl',
  iconType: MarkdownCheckmarkIcon
}];
var quoteCodeLinkButtons = [{
  id: 'mdQuote',
  label: 'Quote',
  name: 'quote',
  iconType: 'quote'
}, {
  id: 'mdCode',
  label: 'Code',
  name: 'code',
  iconType: 'editorCodeBlock'
}, {
  id: 'mdLink',
  label: 'Link',
  name: 'link',
  iconType: 'editorLink'
}];
export var EuiMarkdownEditorToolbar = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var markdownActions = _ref.markdownActions,
      viewMode = _ref.viewMode,
      onClickPreview = _ref.onClickPreview,
      uiPlugins = _ref.uiPlugins,
      selectedNode = _ref.selectedNode;

  var _useContext = useContext(EuiMarkdownContext),
      openPluginEditor = _useContext.openPluginEditor;

  var handleMdButtonClick = function handleMdButtonClick(mdButtonId) {
    var actionResult = markdownActions.do(mdButtonId);
    if (actionResult !== true) openPluginEditor(actionResult);
  };

  var isPreviewing = viewMode === MODE_VIEWING;
  return ___EmotionJSX("div", {
    ref: ref,
    className: "euiMarkdownEditorToolbar"
  }, ___EmotionJSX("div", {
    className: "euiMarkdownEditorToolbar__buttons"
  }, boldItalicButtons.map(function (item) {
    return ___EmotionJSX(EuiToolTip, {
      key: item.id,
      content: item.label,
      delay: "long"
    }, ___EmotionJSX(EuiButtonIcon, {
      color: "text",
      onClick: function onClick() {
        return handleMdButtonClick(item.id);
      },
      iconType: item.iconType,
      "aria-label": item.label,
      isDisabled: isPreviewing
    }));
  }), ___EmotionJSX("span", {
    className: "euiMarkdownEditorToolbar__divider"
  }), listButtons.map(function (item) {
    return ___EmotionJSX(EuiToolTip, {
      key: item.id,
      content: item.label,
      delay: "long"
    }, ___EmotionJSX(EuiButtonIcon, {
      color: "text",
      onClick: function onClick() {
        return handleMdButtonClick(item.id);
      },
      iconType: item.iconType,
      "aria-label": item.label,
      isDisabled: isPreviewing
    }));
  }), ___EmotionJSX("span", {
    className: "euiMarkdownEditorToolbar__divider"
  }), quoteCodeLinkButtons.map(function (item) {
    return ___EmotionJSX(EuiToolTip, {
      key: item.id,
      content: item.label,
      delay: "long"
    }, ___EmotionJSX(EuiButtonIcon, {
      color: "text",
      onClick: function onClick() {
        return handleMdButtonClick(item.id);
      },
      iconType: item.iconType,
      "aria-label": item.label,
      isDisabled: isPreviewing
    }));
  }), uiPlugins.length > 0 ? ___EmotionJSX(React.Fragment, null, ___EmotionJSX("span", {
    className: "euiMarkdownEditorToolbar__divider"
  }), uiPlugins.map(function (_ref2) {
    var name = _ref2.name,
        button = _ref2.button;
    var isSelectedNodeType = selectedNode && selectedNode.type === name;
    return ___EmotionJSX(EuiToolTip, {
      key: name,
      content: button.label,
      delay: "long"
    }, ___EmotionJSX(EuiButtonIcon, _extends({
      color: "text"
    }, isSelectedNodeType ? {
      style: {
        background: 'rgba(0, 0, 0, 0.15)'
      }
    } : null, {
      onClick: function onClick() {
        return handleMdButtonClick(name);
      },
      iconType: button.iconType,
      "aria-label": button.label,
      isDisabled: isPreviewing
    })));
  })) : null), isPreviewing ? ___EmotionJSX(EuiButtonEmpty, {
    iconType: "editorCodeBlock",
    color: "text",
    size: "s",
    onClick: onClickPreview
  }, ___EmotionJSX(EuiI18n, {
    token: "euiMarkdownEditorToolbar.editor",
    default: "Editor"
  })) : ___EmotionJSX(EuiButtonEmpty, {
    iconType: "eye",
    color: "text",
    size: "s",
    onClick: onClickPreview
  }, ___EmotionJSX(EuiI18n, {
    token: "euiMarkdownEditorToolbar.previewMarkdown",
    default: "Preview"
  })));
});
EuiMarkdownEditorToolbar.displayName = 'EuiMarkdownEditorToolbar';