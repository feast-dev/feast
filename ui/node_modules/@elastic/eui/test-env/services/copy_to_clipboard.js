"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.copyToClipboard = copyToClipboard;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
function createHiddenTextElement(text) {
  var textElement = document.createElement('span');
  textElement.textContent = text; // @ts-ignore .all is a real property - https://drafts.csswg.org/css-cascade/#all-shorthand

  textElement.style.all = 'unset'; // prevents scrolling to the end of the page

  textElement.style.position = 'fixed';
  textElement.style.top = '0';
  textElement.style.clip = 'rect(0, 0, 0, 0)'; // used to preserve spaces and line breaks

  textElement.style.whiteSpace = 'pre'; // do not inherit user-select (it may be `none`)

  textElement.style.webkitUserSelect = 'text'; // @ts-ignore this one doesn't appear in the TS definitions for some reason

  textElement.style.MozUserSelect = 'text'; // @ts-ignore this one doesn't appear in the TS definitions for some reason

  textElement.style.msUserSelect = 'text';
  textElement.style.userSelect = 'text';
  return textElement;
}

function copyToClipboard(text) {
  var isCopied = true;
  var range = document.createRange();
  var selection = window.getSelection();
  var elementToBeCopied = createHiddenTextElement(text);
  document.body.appendChild(elementToBeCopied);
  range.selectNode(elementToBeCopied);

  if (selection) {
    selection.removeAllRanges();
    selection.addRange(range);
  }

  if (!document.execCommand('copy')) {
    isCopied = false;
    console.warn('Unable to copy to clipboard.');
  }

  if (selection) {
    if (typeof selection.removeRange === 'function') {
      selection.removeRange(range);
    } else {
      selection.removeAllRanges();
    }
  }

  document.body.removeChild(elementToBeCopied);
  return isCopied;
}