/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { EuiI18n } from '../../../i18n';
import { EuiFlexGrid, EuiFlexItem } from '../../../flex';
import { EuiTitle } from '../../../title';
import { EuiLink } from '../../../link';
import { useGeneratedHtmlId } from '../../../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiCommonlyUsedTimeRanges = function EuiCommonlyUsedTimeRanges(_ref) {
  var applyTime = _ref.applyTime,
      commonlyUsedRanges = _ref.commonlyUsedRanges;
  var legendId = useGeneratedHtmlId();
  var links = commonlyUsedRanges.map(function (_ref2) {
    var start = _ref2.start,
        end = _ref2.end,
        label = _ref2.label;

    var applyCommonlyUsed = function applyCommonlyUsed() {
      applyTime({
        start: start,
        end: end
      });
    };

    var dataTestSubj = label ? "superDatePickerCommonlyUsed_".concat(label.replace(' ', '_')) : undefined;
    return ___EmotionJSX(EuiFlexItem, {
      key: label,
      component: "li",
      className: "euiQuickSelectPopover__sectionItem"
    }, ___EmotionJSX(EuiLink, {
      onClick: applyCommonlyUsed,
      "data-test-subj": dataTestSubj
    }, label));
  });
  return ___EmotionJSX("fieldset", null, ___EmotionJSX(EuiTitle, {
    size: "xxxs"
  }, ___EmotionJSX("legend", {
    id: legendId
  }, ___EmotionJSX(EuiI18n, {
    token: "euiCommonlyUsedTimeRanges.legend",
    default: "Commonly used"
  }))), ___EmotionJSX("div", {
    className: "euiQuickSelectPopover__section"
  }, ___EmotionJSX(EuiFlexGrid, {
    "aria-labelledby": legendId,
    gutterSize: "s",
    columns: 2,
    direction: "column",
    responsive: false,
    component: "ul"
  }, links)));
};
EuiCommonlyUsedTimeRanges.displayName = 'EuiCommonlyUsedTimeRanges';