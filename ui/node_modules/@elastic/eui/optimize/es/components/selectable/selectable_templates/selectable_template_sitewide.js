import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
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
import React, { useState, useEffect } from 'react';
import classNames from 'classnames';
import { useCombinedRefs, throttle } from '../../../services';
import { EuiSelectable } from '../selectable';
import { EuiPopoverTitle, EuiPopoverFooter } from '../../popover';
import { EuiPopover } from '../../popover/popover';
import { useEuiI18n, EuiI18n } from '../../i18n';
import { EuiSelectableMessage } from '../selectable_message';
import { EuiLoadingSpinner } from '../../loading';
import { euiSelectableTemplateSitewideFormatOptions, euiSelectableTemplateSitewideRenderOptions } from './selectable_template_sitewide_option';
import { isWithinBreakpoints } from '../../../services/breakpoint';
import { EuiSpacer } from '../../spacer';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiSelectableTemplateSitewide = function EuiSelectableTemplateSitewide(_ref) {
  var children = _ref.children,
      className = _ref.className,
      options = _ref.options,
      popoverProps = _ref.popoverProps,
      popoverTitle = _ref.popoverTitle,
      popoverFooter = _ref.popoverFooter,
      searchProps = _ref.searchProps,
      listProps = _ref.listProps,
      isLoading = _ref.isLoading,
      popoverButton = _ref.popoverButton,
      popoverButtonBreakpoints = _ref.popoverButtonBreakpoints,
      rest = _objectWithoutProperties(_ref, ["children", "className", "options", "popoverProps", "popoverTitle", "popoverFooter", "searchProps", "listProps", "isLoading", "popoverButton", "popoverButtonBreakpoints"]);

  /**
   * Breakpoint management
   */
  var _useState = useState(typeof window !== 'undefined' && popoverButtonBreakpoints ? isWithinBreakpoints(window.innerWidth, popoverButtonBreakpoints) : true),
      _useState2 = _slicedToArray(_useState, 2),
      canShowPopoverButton = _useState2[0],
      setCanShowPopoverButton = _useState2[1];

  var functionToCallOnWindowResize = throttle(function () {
    var newWidthIsWithinBreakpoint = popoverButtonBreakpoints ? isWithinBreakpoints(window.innerWidth, popoverButtonBreakpoints) : true;

    if (newWidthIsWithinBreakpoint !== canShowPopoverButton) {
      setCanShowPopoverButton(newWidthIsWithinBreakpoint);
    } // reacts every 50ms to resize changes and always gets the final update

  }, 50); // Add window resize handlers

  useEffect(function () {
    window.addEventListener('resize', functionToCallOnWindowResize);
    return function () {
      window.removeEventListener('resize', functionToCallOnWindowResize);
    };
  }, [functionToCallOnWindowResize]);
  /**
   * i18n text
   */

  var _useEuiI18n = useEuiI18n(['euiSelectableTemplateSitewide.searchPlaceholder'], ['Search for anything...']),
      _useEuiI18n2 = _slicedToArray(_useEuiI18n, 1),
      searchPlaceholder = _useEuiI18n2[0];
  /**
   * Popover helpers
   */


  var _useState3 = useState(null),
      _useState4 = _slicedToArray(_useState3, 2),
      popoverRef = _useState4[0],
      setPopoverRef = _useState4[1];

  var _useState5 = useState(false),
      _useState6 = _slicedToArray(_useState5, 2),
      popoverIsOpen = _useState6[0],
      setPopoverIsOpen = _useState6[1];

  var _popoverProps = _objectSpread({}, popoverProps),
      _closePopover = _popoverProps.closePopover,
      panelRef = _popoverProps.panelRef,
      width = _popoverProps.width,
      popoverRest = _objectWithoutProperties(_popoverProps, ["closePopover", "panelRef", "width"]);

  var closePopover = function closePopover() {
    setPopoverIsOpen(false);
    _closePopover && _closePopover();
  };

  var togglePopover = function togglePopover() {
    setPopoverIsOpen(!popoverIsOpen);
  }; // Width applied to the internal div


  var popoverWidth = width || 600;
  var setPanelRef = useCombinedRefs([setPopoverRef, panelRef]);
  /**
   * Search helpers
   */

  var searchOnFocus = function searchOnFocus(e) {
    searchProps && searchProps.onFocus && searchProps.onFocus(e);
    if (canShowPopoverButton) return;
    setPopoverIsOpen(true);
  };

  var onSearchInput = function onSearchInput(e) {
    searchProps && searchProps.onInput && searchProps.onInput(e);
    setPopoverIsOpen(true);
  };

  var searchOnBlur = function searchOnBlur(e) {
    searchProps && searchProps.onBlur && searchProps.onBlur(e);
    if (canShowPopoverButton) return;

    if (!(popoverRef === null || popoverRef === void 0 ? void 0 : popoverRef.contains(e.relatedTarget))) {
      setPopoverIsOpen(false);
    }
  };
  /**
   * Classes
   */


  var classes = classNames('euiSelectableTemplateSitewide', className);
  var searchClasses = classNames('euiSelectableTemplateSitewide__search', searchProps && searchProps.className);
  var listClasses = classNames('euiSelectableTemplateSitewide__list', listProps && listProps.className);
  /**
   * List options
   */

  var formattedOptions = euiSelectableTemplateSitewideFormatOptions(options);

  var loadingMessage = ___EmotionJSX(EuiSelectableMessage, {
    style: {
      minHeight: 300
    }
  }, ___EmotionJSX(EuiLoadingSpinner, {
    size: "l"
  }), ___EmotionJSX("br", null), ___EmotionJSX("p", null, ___EmotionJSX(EuiI18n, {
    token: "euiSelectableTemplateSitewide.loadingResults",
    default: "Loading results"
  })));

  var emptyMessage = ___EmotionJSX(EuiSelectableMessage, {
    style: {
      minHeight: 300
    }
  }, ___EmotionJSX("p", null, ___EmotionJSX(EuiI18n, {
    token: "euiSelectableTemplateSitewide.noResults",
    default: "No results available"
  })));
  /**
   * Changes based on showing the `popoverButton` if provided.
   * This will move the search input into the popover
   * and use the passed `popoverButton` as the popover trigger.
   */


  var popoverTrigger;

  if (popoverButton && canShowPopoverButton) {
    popoverTrigger = /*#__PURE__*/React.cloneElement(popoverButton, _objectSpread(_objectSpread({}, popoverButton.props), {}, {
      onClick: togglePopover,
      onKeyDown: function onKeyDown(e) {
        // Selectable preventsDefault on Enter which kills browser controls for pressing the button
        e.stopPropagation();
      }
    }));
  }

  return ___EmotionJSX(EuiSelectable, _extends({
    isLoading: isLoading,
    options: formattedOptions,
    renderOption: euiSelectableTemplateSitewideRenderOptions,
    singleSelection: true,
    searchProps: _objectSpread(_objectSpread({
      placeholder: searchPlaceholder,
      isClearable: true
    }, searchProps), {}, {
      onFocus: searchOnFocus,
      onBlur: searchOnBlur,
      onInput: onSearchInput,
      className: searchClasses
    }),
    listProps: _objectSpread(_objectSpread({
      rowHeight: 68,
      showIcons: false,
      onFocusBadge: {
        iconSide: 'right',
        children: ___EmotionJSX(EuiI18n, {
          token: "euiSelectableTemplateSitewide.onFocusBadgeGoTo",
          default: "Go to"
        })
      }
    }, listProps), {}, {
      className: listClasses
    }),
    loadingMessage: loadingMessage,
    emptyMessage: emptyMessage,
    noMatchesMessage: emptyMessage
  }, rest, {
    className: classes,
    searchable: true
  }), function (list, search) {
    return ___EmotionJSX(EuiPopover, _extends({
      panelPaddingSize: "none",
      isOpen: popoverIsOpen,
      ownFocus: !!popoverTrigger,
      display: popoverTrigger ? 'inlineBlock' : 'block'
    }, popoverRest, {
      panelRef: setPanelRef,
      button: popoverTrigger ? popoverTrigger : search,
      closePopover: closePopover
    }), ___EmotionJSX("div", {
      style: {
        width: popoverWidth,
        maxWidth: '100%'
      }
    }, popoverTitle || popoverTrigger ? ___EmotionJSX(EuiPopoverTitle, {
      paddingSize: "s"
    }, popoverTitle, popoverTitle && search && ___EmotionJSX(EuiSpacer, null), search) : undefined, list, popoverFooter && ___EmotionJSX(EuiPopoverFooter, {
      paddingSize: "s"
    }, popoverFooter)));
  });
};