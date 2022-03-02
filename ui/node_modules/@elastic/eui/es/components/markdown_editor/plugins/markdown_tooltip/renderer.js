/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import PropTypes from "prop-types";
import { EuiToolTip } from '../../../tool_tip';
import { EuiIcon } from '../../../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var tooltipMarkdownRenderer = function tooltipMarkdownRenderer(_ref) {
  var content = _ref.content,
      children = _ref.children;
  return ___EmotionJSX("span", null, ___EmotionJSX(EuiToolTip, {
    content: content
  }, ___EmotionJSX("span", null, ___EmotionJSX("strong", null, children), ___EmotionJSX(EuiIcon, {
    type: "questionInCircle",
    className: "euiMarkdownTooltip__icon"
  }))));
};
tooltipMarkdownRenderer.propTypes = {
  type: PropTypes.oneOf(["tooltipPlugin"]).isRequired,
  content: PropTypes.string.isRequired,
  position: PropTypes.shape({
    start: PropTypes.shape({
      line: PropTypes.number.isRequired,
      column: PropTypes.number.isRequired,
      offset: PropTypes.number.isRequired
    }).isRequired,
    end: PropTypes.shape({
      line: PropTypes.number.isRequired,
      column: PropTypes.number.isRequired,
      offset: PropTypes.number.isRequired
    }).isRequired
  }).isRequired
};