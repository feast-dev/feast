function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import PropTypes from "prop-types";
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
EuiTableHeaderCell.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  align: PropTypes.oneOf(["left", "right", "center"]),
  isSortAscending: PropTypes.bool,
  isSorted: PropTypes.bool,

  /**
       * Mobile options for displaying differently at small screens
       */
  mobileOptions: PropTypes.shape({
    /**
           * If false, will not render the column at all for mobile
           */
    show: PropTypes.bool,

    /**
           * Only show for mobile? If true, will not render the column at all
           * for desktop
           */
    only: PropTypes.bool
  }),
  onSort: PropTypes.func,
  scope: PropTypes.oneOf(["col", "row", "colgroup", "rowgroup"]),
  width: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired]),
  description: PropTypes.string,

  /**
       * Shows the sort indicator but removes the button
       */
  readOnly: PropTypes.bool
};