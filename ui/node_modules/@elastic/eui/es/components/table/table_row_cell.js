function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Fragment } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { LEFT_ALIGNMENT, RIGHT_ALIGNMENT, CENTER_ALIGNMENT } from '../../services';
import { resolveWidthAsStyle } from './utils';
import { useIsWithinBreakpoints } from '../../services/hooks/useIsWithinBreakpoints';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiTableRowCell = function EuiTableRowCell(_ref) {
  var _mobileOptions$showOn, _mobileOptions$trunca;

  var _ref$align = _ref.align,
      align = _ref$align === void 0 ? LEFT_ALIGNMENT : _ref$align,
      children = _ref.children,
      className = _ref.className,
      truncateText = _ref.truncateText,
      setScopeRow = _ref.setScopeRow,
      showOnHover = _ref.showOnHover,
      _ref$textOnly = _ref.textOnly,
      textOnly = _ref$textOnly === void 0 ? true : _ref$textOnly,
      hasActions = _ref.hasActions,
      isExpander = _ref.isExpander,
      style = _ref.style,
      width = _ref.width,
      _ref$valign = _ref.valign,
      valign = _ref$valign === void 0 ? 'middle' : _ref$valign,
      _ref$mobileOptions = _ref.mobileOptions,
      mobileOptions = _ref$mobileOptions === void 0 ? {
    show: true
  } : _ref$mobileOptions,
      rest = _objectWithoutProperties(_ref, ["align", "children", "className", "truncateText", "setScopeRow", "showOnHover", "textOnly", "hasActions", "isExpander", "style", "width", "valign", "mobileOptions"]);

  var cellClasses = classNames('euiTableRowCell', _defineProperty({
    'euiTableRowCell--hasActions': hasActions,
    'euiTableRowCell--isExpander': isExpander,
    'euiTableRowCell--hideForDesktop': mobileOptions.only,
    'euiTableRowCell--enlargeForMobile': mobileOptions.enlarge
  }, "euiTableRowCell--".concat(valign), valign));
  var contentClasses = classNames('euiTableCellContent', className, {
    'euiTableCellContent--alignRight': align === RIGHT_ALIGNMENT,
    'euiTableCellContent--alignCenter': align === CENTER_ALIGNMENT,
    'euiTableCellContent--showOnHover': showOnHover,
    'euiTableCellContent--truncateText': truncateText,
    // We're doing this rigamarole instead of creating `euiTableCellContent--textOnly` for BWC
    // purposes for the time-being.
    'euiTableCellContent--overflowingContent': textOnly !== true
  });
  var mobileContentClasses = classNames('euiTableCellContent', className, {
    'euiTableCellContent--alignRight': mobileOptions.align === RIGHT_ALIGNMENT || align === RIGHT_ALIGNMENT,
    'euiTableCellContent--alignCenter': mobileOptions.align === CENTER_ALIGNMENT || align === CENTER_ALIGNMENT,
    'euiTableCellContent--showOnHover': (_mobileOptions$showOn = mobileOptions.showOnHover) !== null && _mobileOptions$showOn !== void 0 ? _mobileOptions$showOn : showOnHover,
    'euiTableCellContent--truncateText': (_mobileOptions$trunca = mobileOptions.truncateText) !== null && _mobileOptions$trunca !== void 0 ? _mobileOptions$trunca : truncateText,
    // We're doing this rigamarole instead of creating `euiTableCellContent--textOnly` for BWC
    // purposes for the time-being.
    'euiTableCellContent--overflowingContent': mobileOptions.textOnly !== true || textOnly !== true
  });
  var childClasses = classNames({
    euiTableCellContent__text: textOnly === true,
    euiTableCellContent__hoverItem: showOnHover
  });
  var widthValue = useIsWithinBreakpoints(['xs', 's', 'm']) && mobileOptions.width ? mobileOptions.width : width;
  var styleObj = resolveWidthAsStyle(style, widthValue);

  function modifyChildren(children) {
    var modifiedChildren = children;

    if (textOnly === true) {
      modifiedChildren = ___EmotionJSX("span", {
        className: childClasses
      }, children);
    } else if ( /*#__PURE__*/React.isValidElement(children)) {
      modifiedChildren = React.Children.map(children, function (child) {
        return /*#__PURE__*/React.cloneElement(child, {
          className: classNames(child.props.className, childClasses)
        });
      });
    }

    return modifiedChildren;
  }

  var childrenNode = modifyChildren(children);
  var hideForMobileClasses = 'euiTableRowCell--hideForMobile';
  var showForMobileClasses = 'euiTableRowCell--hideForDesktop';
  var Element = setScopeRow ? 'th' : 'td';

  var sharedProps = _objectSpread({
    scope: setScopeRow ? 'row' : undefined,
    style: styleObj
  }, rest);

  if (mobileOptions.show === false) {
    return ___EmotionJSX(Element, _extends({
      className: "".concat(cellClasses, " ").concat(hideForMobileClasses)
    }, sharedProps), ___EmotionJSX("div", {
      className: contentClasses
    }, childrenNode));
  } else {
    return ___EmotionJSX(Element, _extends({
      className: cellClasses
    }, sharedProps), mobileOptions.header && ___EmotionJSX("div", {
      className: "euiTableRowCell__mobileHeader ".concat(showForMobileClasses)
    }, mobileOptions.header), mobileOptions.render ? ___EmotionJSX(Fragment, null, ___EmotionJSX("div", {
      className: "".concat(mobileContentClasses, " ").concat(showForMobileClasses)
    }, modifyChildren(mobileOptions.render)), ___EmotionJSX("div", {
      className: "".concat(contentClasses, " ").concat(hideForMobileClasses)
    }, childrenNode)) : ___EmotionJSX("div", {
      className: contentClasses
    }, childrenNode));
  }
};
EuiTableRowCell.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Vertical alignment of the content in the cell
     */
  valign: PropTypes.any,

  /**
     * Indicates whether the cell should be marked as the heading for its row
     */
  setScopeRow: PropTypes.bool,

  /**
     * Indicates if the column is dedicated to icon-only actions (currently
     * affects mobile only)
     */
  hasActions: PropTypes.bool,

  /**
     * Indicates if the column is dedicated as the expandable row toggle
     */
  isExpander: PropTypes.bool,

  /**
     * Mobile options for displaying differently at small screens;
     * See #EuiTableRowCellMobileOptionsShape
     */
  mobileOptions: PropTypes.shape({
    /**
       * If false, will not render the cell at all for mobile
       */
    show: PropTypes.bool,

    /**
       * Only show for mobile? If true, will not render the column at all for desktop
       */
    only: PropTypes.bool,

    /**
       * Custom render/children if different from desktop
       */
    render: PropTypes.node,

    /**
       * The column's header for use in mobile view (automatically passed down
       * when using `EuiBasicTable`).
       * Or pass `false` to not show a header at all.
       */
    header: PropTypes.oneOfType([PropTypes.node.isRequired, PropTypes.bool.isRequired]),

    /**
       * Increase text size compared to rest of cells
       */
    enlarge: PropTypes.bool,

    /**
       * Applies the value to the width of the cell in mobile view (typically 50%)
       */
    width: PropTypes.any,

    /**
       * Horizontal alignment of the text in the cell
       */
    align: PropTypes.oneOf(["left", "right", "center"]),

    /**
       * _Should only be used for action cells_
       */
    showOnHover: PropTypes.bool,

    /**
       * Setting `textOnly` to `false` will break words unnecessarily on FF and
       * IE.  To combat this problem on FF, wrap contents with the css utility
       * `.eui-textBreakWord`.
       */
    textOnly: PropTypes.bool,

    /**
       * Don't allow line breaks within cells
       */
    truncateText: PropTypes.bool
  }),

  /**
     * Horizontal alignment of the text in the cell
     */
  align: PropTypes.oneOf(["left", "right", "center"]),

  /**
     * _Should only be used for action cells_
     */
  showOnHover: PropTypes.bool,

  /**
     * Setting `textOnly` to `false` will break words unnecessarily on FF and
     * IE.  To combat this problem on FF, wrap contents with the css utility
     * `.eui-textBreakWord`.
     */
  textOnly: PropTypes.bool,

  /**
     * Don't allow line breaks within cells
     */
  truncateText: PropTypes.bool,
  width: PropTypes.any
};