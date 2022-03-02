/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { EuiCodeBlock } from '../../../code';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var tooltipPlugin = {
  name: 'tooltipPlugin',
  button: {
    label: 'Tooltip',
    iconType: 'editorComment'
  },
  formatting: {
    prefix: '!{tooltip[',
    suffix: ']()}',
    trimFirst: true
  },
  helpText: ___EmotionJSX(EuiCodeBlock, {
    language: "md",
    paddingSize: "s",
    fontSize: "l"
  }, '!{tooltip[anchor text](helpful description)}')
};