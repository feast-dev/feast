import _extends from "@babel/runtime/helpers/extends";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useEffect } from 'react';
import classNames from 'classnames';
import { useDropzone } from 'react-dropzone';
import { EuiMarkdownEditorFooter } from './markdown_editor_footer';
import { useResizeObserver } from '../observer/resize_observer';
import { jsx as ___EmotionJSX } from "@emotion/react";

var getUnacceptedItems = function getUnacceptedItems(items, dropHandlers) {
  var unacceptedItems = [];

  for (var i = 0; i < items.length; i++) {
    var item = items[i];
    var isAccepted = false;

    for (var j = 0; j < dropHandlers.length; j++) {
      if (dropHandlers[j].accepts(item.type)) {
        isAccepted = true;
        break;
      }
    }

    if (!isAccepted) {
      unacceptedItems.push(item);
    }
  }

  return unacceptedItems;
};

export var EuiMarkdownEditorDropZone = function EuiMarkdownEditorDropZone(props) {
  var _React$useState = React.useState(false),
      _React$useState2 = _slicedToArray(_React$useState, 2),
      isDragging = _React$useState2[0],
      toggleDragging = _React$useState2[1];

  var _React$useState3 = React.useState(false),
      _React$useState4 = _slicedToArray(_React$useState3, 2),
      isUploadingFiles = _React$useState4[0],
      toggleUploadingFiles = _React$useState4[1];

  var _React$useState5 = React.useState(false),
      _React$useState6 = _slicedToArray(_React$useState5, 2),
      isDraggingError = _React$useState6[0],
      toggleDraggingError = _React$useState6[1];

  var children = props.children,
      uiPlugins = props.uiPlugins,
      errors = props.errors,
      dropHandlers = props.dropHandlers,
      insertText = props.insertText,
      hasUnacceptedItems = props.hasUnacceptedItems,
      setHasUnacceptedItems = props.setHasUnacceptedItems,
      setEditorFooterHeight = props.setEditorFooterHeight,
      isEditing = props.isEditing;
  var classes = classNames('euiMarkdownEditorDropZone', {
    'euiMarkdownEditorDropZone--isDragging': isDragging,
    'euiMarkdownEditorDropZone--hasError': hasUnacceptedItems,
    'euiMarkdownEditorDropZone--isDraggingError': isDraggingError
  });

  var _React$useState7 = React.useState(null),
      _React$useState8 = _slicedToArray(_React$useState7, 2),
      editorFooterRef = _React$useState8[0],
      setEditorFooterRef = _React$useState8[1];

  var _useResizeObserver = useResizeObserver(editorFooterRef, 'height'),
      editorFooterHeight = _useResizeObserver.height;

  useEffect(function () {
    if (editorFooterHeight !== 0) {
      setEditorFooterHeight(editorFooterHeight);
    }
  }, [setEditorFooterHeight, isEditing, editorFooterHeight]);

  var _useDropzone = useDropzone({
    disabled: dropHandlers.length === 0,
    // Disable click and keydown behavior
    noClick: true,
    noKeyboard: true,
    preventDropOnDocument: false,
    // multiple: false,
    onDragOver: function onDragOver(e) {
      var result;

      if (e.dataTransfer) {
        var unacceptedItems = getUnacceptedItems(e.dataTransfer.items, dropHandlers);
        setHasUnacceptedItems(unacceptedItems.length > 0);
        toggleDraggingError(unacceptedItems.length > 0);
        result = unacceptedItems.length === 0;
      } else {
        setHasUnacceptedItems(false);
        result = false;
      }

      toggleDragging(result);

      if (result === false) {
        e.preventDefault();
      }

      return result;
    },
    onDragEnter: function onDragEnter(e) {
      var result;

      if (e.dataTransfer) {
        var unacceptedItems = getUnacceptedItems(e.dataTransfer.items, dropHandlers);
        setHasUnacceptedItems(unacceptedItems.length > 0);
        toggleDraggingError(unacceptedItems.length > 0);
        result = unacceptedItems.length === 0;
      } else {
        setHasUnacceptedItems(false);
        result = false;
      }

      toggleDragging(result);

      if (result === false) {
        e.preventDefault();
      }

      return result;
    },
    onDragLeave: function onDragLeave() {
      toggleDragging(false);
    },
    onDrop: function onDrop(acceptedFiles) {
      var fileHandlers = []; // verify all files being dropped are supported

      preparation: for (var i = 0; i < acceptedFiles.length; i++) {
        var file = acceptedFiles[i];

        for (var j = 0; j < dropHandlers.length; j++) {
          if (dropHandlers[j].accepts(file.type)) {
            fileHandlers.push(dropHandlers[j]);
            continue preparation;
          }
        } // if we get here then a file isn't handled


        setHasUnacceptedItems(true);
        toggleDragging(false);
        toggleDraggingError(false);
        return;
      }

      toggleUploadingFiles(true);
      var resolved = [];

      for (var _i = 0; _i < acceptedFiles.length; _i++) {
        var _file = acceptedFiles[_i];
        var handler = fileHandlers[_i];
        resolved.push(handler.getFormattingForItem(_file));
      }

      Promise.all(resolved).then(function (results) {
        results.forEach(function (_ref) {
          var text = _ref.text,
              config = _ref.config;
          return insertText(text, config);
        });
      }).catch(function () {}).then(function () {
        toggleDragging(false);
        toggleUploadingFiles(false);
        toggleDraggingError(false);
      });
    }
  }),
      getRootProps = _useDropzone.getRootProps,
      getInputProps = _useDropzone.getInputProps,
      open = _useDropzone.open;

  return ___EmotionJSX("div", _extends({}, getRootProps(), {
    className: classes
  }), children, ___EmotionJSX(EuiMarkdownEditorFooter, {
    ref: setEditorFooterRef,
    uiPlugins: uiPlugins,
    openFiles: function openFiles() {
      setHasUnacceptedItems(false);
      open();
    },
    isUploadingFiles: isUploadingFiles,
    hasUnacceptedItems: hasUnacceptedItems,
    dropHandlers: dropHandlers,
    errors: errors
  }), ___EmotionJSX("input", getInputProps()));
};