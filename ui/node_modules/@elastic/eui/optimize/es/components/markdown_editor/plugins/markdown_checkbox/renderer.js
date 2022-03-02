/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useContext } from 'react';
import { EuiCheckbox } from '../../../form/checkbox';
import { EuiMarkdownContext } from '../../markdown_context';
import { useGeneratedHtmlId } from '../../../../services/accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var CheckboxMarkdownRenderer = function CheckboxMarkdownRenderer(_ref) {
  var position = _ref.position,
      lead = _ref.lead,
      label = _ref.label,
      isChecked = _ref.isChecked,
      children = _ref.children;

  var _useContext = useContext(EuiMarkdownContext),
      replaceNode = _useContext.replaceNode;

  return ___EmotionJSX(EuiCheckbox, {
    id: useGeneratedHtmlId(),
    checked: isChecked,
    label: children,
    onChange: function onChange() {
      replaceNode(position, "".concat(lead, "[").concat(isChecked ? ' ' : 'x', "]").concat(label));
    }
  });
};