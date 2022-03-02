import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import classNames from 'classnames';
import { EuiScreenReaderOnly } from '../accessibility';
import { EuiIcon } from '../icon';
import { resolveWidthAsStyle } from './utils';
import { EuiInnerText } from '../inner_text';
import { LEFT_ALIGNMENT, RIGHT_ALIGNMENT, CENTER_ALIGNMENT } from '../../services';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";

var CellContents = function CellContents(_ref) {
  var className = _ref.className,
      description = _ref.description,
      children = _ref.children,
      isSorted = _ref.isSorted,
      isSortAscending = _ref.isSortAscending,
      showSortMsg = _ref.showSortMsg;
  return ___EmotionJSX("span", {
    className: className
  }, ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
    return ___EmotionJSX(EuiI18n, {
      token: "euiTableHeaderCell.titleTextWithDesc",
      default: "{innerText}; {description}",
      values: {
        innerText: innerText,
        description: description
      }
    }, function (titleTextWithDesc) {
      return ___EmotionJSX("span", {
        title: description ? titleTextWithDesc : innerText,
        ref: ref,
        className: "euiTableCellContent__text"
      }, children);
    });
  }), description && ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, description)), showSortMsg && isSorted && ___EmotionJSX(EuiIcon, {
    className: "euiTableSortIcon",
    type: isSortAscending ? 'sortUp' : 'sortDown',
    size: "m"
  }));
};

export var EuiTableHeaderCell = function EuiTableHeaderCell(_ref2) {
  var children = _ref2.children,
      _ref2$align = _ref2.align,
      align = _ref2$align === void 0 ? LEFT_ALIGNMENT : _ref2$align,
      onSort = _ref2.onSort,
      isSorted = _ref2.isSorted,
      isSortAscending = _ref2.isSortAscending,
      className = _ref2.className,
      scope = _ref2.scope,
      _ref2$mobileOptions = _ref2.mobileOptions,
      mobileOptions = _ref2$mobileOptions === void 0 ? {
    show: true
  } : _ref2$mobileOptions,
      width = _ref2.width,
      style = _ref2.style,
      readOnly = _ref2.readOnly,
      description = _ref2.description,
      rest = _objectWithoutProperties(_ref2, ["children", "align", "onSort", "isSorted", "isSortAscending", "className", "scope", "mobileOptions", "width", "style", "readOnly", "description"]);

  var classes = classNames('euiTableHeaderCell', className, {
    'euiTableHeaderCell--hideForDesktop': mobileOptions.only,
    'euiTableHeaderCell--hideForMobile': !mobileOptions.show
  });
  var contentClasses = classNames('euiTableCellContent', className, {
    'euiTableCellContent--alignRight': align === RIGHT_ALIGNMENT,
    'euiTableCellContent--alignCenter': align === CENTER_ALIGNMENT
  });
  var styleObj = resolveWidthAsStyle(style, width);
  var CellComponent = children ? 'th' : 'td';
  var cellScope = CellComponent === 'th' ? scope !== null && scope !== void 0 ? scope : 'col' : undefined; // `scope` is only valid on `th` elements

  if (onSort || isSorted) {
    var buttonClasses = classNames('euiTableHeaderButton', {
      'euiTableHeaderButton-isSorted': isSorted
    });
    var ariaSortValue = 'none';

    if (isSorted) {
      ariaSortValue = isSortAscending ? 'ascending' : 'descending';
    }

    var cellContents = ___EmotionJSX(CellContents, {
      className: contentClasses,
      description: description,
      showSortMsg: true,
      children: children,
      isSorted: isSorted,
      isSortAscending: isSortAscending
    });

    return ___EmotionJSX(CellComponent, _extends({
      className: classes,
      scope: cellScope,
      role: "columnheader",
      "aria-sort": ariaSortValue,
      "aria-live": "polite",
      style: styleObj
    }, rest), onSort && !readOnly ? ___EmotionJSX("button", {
      type: "button",
      className: buttonClasses,
      onClick: onSort,
      "data-test-subj": "tableHeaderSortButton"
    }, cellContents) : cellContents);
  }

  return ___EmotionJSX(CellComponent, _extends({
    className: classes,
    scope: cellScope,
    role: "columnheader",
    style: styleObj
  }, rest), ___EmotionJSX(CellContents, {
    className: contentClasses,
    description: description,
    showSortMsg: false,
    children: children,
    isSorted: isSorted,
    isSortAscending: isSortAscending
  }));
};