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
import React, { Fragment } from 'react';
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