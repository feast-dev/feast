/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { prettyDuration } from '../pretty_duration';
import { EuiI18n } from '../../../i18n';
import { useGeneratedHtmlId } from '../../../../services';
import { EuiTitle } from '../../../title';
import { EuiLink } from '../../../link';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiRecentlyUsed = function EuiRecentlyUsed(_ref) {
  var applyTime = _ref.applyTime,
      commonlyUsedRanges = _ref.commonlyUsedRanges,
      dateFormat = _ref.dateFormat,
      _ref$recentlyUsedRang = _ref.recentlyUsedRanges,
      recentlyUsedRanges = _ref$recentlyUsedRang === void 0 ? [] : _ref$recentlyUsedRang;
  var legendId = useGeneratedHtmlId();

  if (recentlyUsedRanges.length === 0) {
    return null;
  }

  var links = recentlyUsedRanges.map(function (_ref2) {
    var start = _ref2.start,
        end = _ref2.end;

    var applyRecentlyUsed = function applyRecentlyUsed() {
      applyTime({
        start: start,
        end: end
      });
    };

    return ___EmotionJSX("li", {
      className: "euiQuickSelectPopover__sectionItem",
      key: "".concat(start, "-").concat(end)
    }, ___EmotionJSX(EuiLink, {
      onClick: applyRecentlyUsed
    }, prettyDuration(start, end, commonlyUsedRanges, dateFormat)));
  });
  return ___EmotionJSX("fieldset", null, ___EmotionJSX(EuiTitle, {
    size: "xxxs"
  }, ___EmotionJSX("legend", {
    id: legendId
  }, ___EmotionJSX(EuiI18n, {
    token: "euiRecentlyUsed.legend",
    default: "Recently used date ranges"
  }))), ___EmotionJSX("div", {
    className: "euiQuickSelectPopover__section"
  }, ___EmotionJSX("ul", null, links)));
};
EuiRecentlyUsed.displayName = 'EuiRecentlyUsed';