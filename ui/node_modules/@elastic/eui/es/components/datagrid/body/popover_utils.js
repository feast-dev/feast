/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { EuiText } from '../../text';
import { EuiCodeBlock } from '../../code';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var DefaultColumnFormatter = function DefaultColumnFormatter(_ref) {
  var children = _ref.children;
  return ___EmotionJSX(EuiText, null, children);
};
export var providedPopoverContents = {
  json: function json(_ref2) {
    var cellContentsElement = _ref2.cellContentsElement;
    var formattedText = cellContentsElement.innerText; // attempt to pretty-print the json

    try {
      formattedText = JSON.stringify(JSON.parse(formattedText), null, 2);
    } catch (e) {} // eslint-disable-line no-empty


    return ___EmotionJSX(EuiCodeBlock, {
      isCopyable: true,
      transparentBackground: true,
      paddingSize: "none",
      language: "json"
    }, formattedText);
  }
};