import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import classNames from 'classnames';
import { EuiButtonEmpty } from '../button';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiPaginationButton = function EuiPaginationButton(_ref) {
  var className = _ref.className,
      isActive = _ref.isActive,
      isPlaceholder = _ref.isPlaceholder,
      pageIndex = _ref.pageIndex,
      totalPages = _ref.totalPages,
      rest = _objectWithoutProperties(_ref, ["className", "isActive", "isPlaceholder", "pageIndex", "totalPages"]);

  var classes = classNames('euiPaginationButton', className, {
    'euiPaginationButton-isActive': isActive,
    'euiPaginationButton-isPlaceholder': isPlaceholder
  });

  var props = _objectSpread(_objectSpread(_objectSpread({
    className: classes,
    size: 's',
    color: 'text',
    'data-test-subj': "pagination-button-".concat(pageIndex),
    isDisabled: isPlaceholder || isActive
  }, isActive && {
    'aria-current': true
  }), rest['aria-controls'] && {
    href: "#".concat(rest['aria-controls'])
  }), rest);

  var pageNumber = pageIndex + 1;
  return ___EmotionJSX(EuiI18n, {
    token: "euiPaginationButton.longPageString",
    default: "Page {page} of {totalPages}",
    values: {
      page: pageNumber,
      totalPages: totalPages
    }
  }, function (longPageString) {
    return ___EmotionJSX(EuiI18n, {
      token: "euiPaginationButton.shortPageString",
      default: "Page {page}",
      values: {
        page: pageNumber
      }
    }, function (shortPageString) {
      return ___EmotionJSX(EuiButtonEmpty, _extends({
        "aria-label": totalPages ? longPageString : shortPageString
      }, props), pageNumber);
    });
  });
};