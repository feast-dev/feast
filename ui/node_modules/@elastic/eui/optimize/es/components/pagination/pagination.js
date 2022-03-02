import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _extends from "@babel/runtime/helpers/extends";
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
import { EuiPaginationButton } from './pagination_button';
import { EuiI18n, useEuiI18n } from '../i18n';
import { EuiText } from '../text';
import { EuiPaginationButtonArrow } from './pagination_button_arrow';
import { useIsWithinBreakpoints } from '../../services';
import { EuiScreenReaderOnly } from '../accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";
var MAX_VISIBLE_PAGES = 5;
var NUMBER_SURROUNDING_PAGES = Math.floor(MAX_VISIBLE_PAGES * 0.5);
export var EuiPagination = function EuiPagination(_ref) {
  var className = _ref.className,
      _ref$pageCount = _ref.pageCount,
      pageCount = _ref$pageCount === void 0 ? 1 : _ref$pageCount,
      _ref$activePage = _ref.activePage,
      activePage = _ref$activePage === void 0 ? 0 : _ref$activePage,
      _ref$onPageClick = _ref.onPageClick,
      onPageClick = _ref$onPageClick === void 0 ? function () {} : _ref$onPageClick,
      _compressed = _ref.compressed,
      ariaControls = _ref['aria-controls'],
      _ref$responsive = _ref.responsive,
      responsive = _ref$responsive === void 0 ? ['xs', 's'] : _ref$responsive,
      rest = _objectWithoutProperties(_ref, ["className", "pageCount", "activePage", "onPageClick", "compressed", "aria-controls", "responsive"]);

  var isResponsive = useIsWithinBreakpoints(responsive, !!responsive); // Force to `compressed` version if specified or within the responsive breakpoints

  var compressed = _compressed || isResponsive;

  var safeClick = function safeClick(e, pageIndex) {
    e.preventDefault();

    if (ariaControls) {
      var controlledElement = document.getElementById(ariaControls);

      if (controlledElement) {
        controlledElement.focus();
      }
    }

    onPageClick(pageIndex);
  };

  var classes = classNames('euiPagination', className);

  var firstButton = (pageCount < 1 || compressed) && ___EmotionJSX(EuiPaginationButtonArrow, {
    type: "first",
    ariaControls: ariaControls,
    onClick: function onClick(e) {
      return safeClick(e, 0);
    },
    disabled: activePage === 0
  });

  var previousButton = ___EmotionJSX(EuiPaginationButtonArrow, {
    type: "previous",
    ariaControls: ariaControls,
    onClick: function onClick(e) {
      return safeClick(e, activePage - 1);
    },
    disabled: activePage === 0
  });

  var nextButton = ___EmotionJSX(EuiPaginationButtonArrow, {
    type: "next",
    ariaControls: ariaControls,
    onClick: function onClick(e) {
      return safeClick(e, activePage + 1);
    },
    disabled: activePage === -1 || activePage === pageCount - 1
  });

  var lastButton = (pageCount < 1 || compressed) && ___EmotionJSX(EuiPaginationButtonArrow, {
    type: "last",
    ariaControls: ariaControls,
    onClick: function onClick(e) {
      return safeClick(e, pageCount ? pageCount - 1 : -1);
    },
    disabled: activePage === -1 || activePage === pageCount - 1
  });

  var centerPageCount;

  if (pageCount) {
    var sharedButtonProps = {
      activePage: activePage,
      ariaControls: ariaControls,
      safeClick: safeClick,
      pageCount: pageCount
    };

    if (compressed) {
      centerPageCount = ___EmotionJSX(EuiText, {
        size: "s",
        className: "euiPagination__compressedText"
      }, ___EmotionJSX(EuiI18n, {
        token: "euiPagination.pageOfTotalCompressed",
        default: "{page} of {total}",
        values: {
          page: ___EmotionJSX("span", null, activePage + 1),
          total: ___EmotionJSX("span", null, pageCount)
        }
      }));
    } else {
      var pages = [];
      var firstPageInRange = Math.max(0, Math.min(activePage - NUMBER_SURROUNDING_PAGES, pageCount - MAX_VISIBLE_PAGES));
      var lastPageInRange = Math.min(pageCount, firstPageInRange + MAX_VISIBLE_PAGES);

      for (var i = firstPageInRange, index = 0; i < lastPageInRange; i++, index++) {
        pages.push(___EmotionJSX(PaginationButtonWrapper, _extends({
          pageIndex: i,
          key: i
        }, sharedButtonProps)));
      }

      var firstPageButtons = [];

      if (firstPageInRange > 0) {
        firstPageButtons.push(___EmotionJSX(PaginationButtonWrapper, _extends({
          pageIndex: 0,
          key: 0
        }, sharedButtonProps)));

        if (firstPageInRange > 1 && firstPageInRange !== 2) {
          firstPageButtons.push(___EmotionJSX(EuiI18n, {
            key: "startingEllipses",
            token: "euiPagination.firstRangeAriaLabel",
            default: "Skipping pages 2 to {lastPage}",
            values: {
              lastPage: firstPageInRange
            }
          }, function (firstRangeAriaLabel) {
            return ___EmotionJSX("li", {
              "aria-label": firstRangeAriaLabel,
              className: "euiPaginationButton-isPlaceholder euiPagination__item"
            }, "\u2026");
          }));
        } else if (firstPageInRange === 2) {
          firstPageButtons.push(___EmotionJSX(PaginationButtonWrapper, _extends({
            pageIndex: 1,
            key: 1
          }, sharedButtonProps)));
        }
      }

      var lastPageButtons = [];

      if (lastPageInRange < pageCount) {
        if (lastPageInRange + 1 === pageCount - 1) {
          lastPageButtons.push(___EmotionJSX(PaginationButtonWrapper, _extends({
            pageIndex: lastPageInRange,
            key: lastPageInRange
          }, sharedButtonProps)));
        } else if (lastPageInRange < pageCount - 1) {
          lastPageButtons.push(___EmotionJSX(EuiI18n, {
            key: "endingEllipses",
            token: "euiPagination.lastRangeAriaLabel",
            default: "Skipping pages {firstPage} to {lastPage}",
            values: {
              firstPage: lastPageInRange + 1,
              lastPage: pageCount - 1
            }
          }, function (lastRangeAriaLabel) {
            return ___EmotionJSX("li", {
              "aria-label": lastRangeAriaLabel,
              className: "euiPaginationButton-isPlaceholder euiPagination__item"
            }, "\u2026");
          }));
        }

        lastPageButtons.push(___EmotionJSX(PaginationButtonWrapper, _extends({
          pageIndex: pageCount - 1,
          key: pageCount - 1
        }, sharedButtonProps)));
      }

      var selectablePages = pages;

      var accessibleName = _objectSpread(_objectSpread({}, rest['aria-label'] && {
        'aria-label': rest['aria-label']
      }), rest['aria-labelledby'] && {
        'aria-labelledby': rest['aria-labelledby']
      });

      centerPageCount = ___EmotionJSX("ul", _extends({}, accessibleName, {
        className: "euiPagination__list"
      }), firstPageButtons, selectablePages, lastPageButtons);
    }
  } // All the i18n strings used to build the whole SR-only text


  var lastLabel = useEuiI18n('euiPagination.last', 'Last');
  var pageLabel = useEuiI18n('euiPagination.page', 'Page');
  var ofLabel = useEuiI18n('euiPagination.of', 'of');
  var collectionLabel = useEuiI18n('euiPagination.collection', 'collection');
  var fromEndLabel = useEuiI18n('euiPagination.fromEndLabel', 'from end'); // Based on the `activePage` count, build the front of the SR-only text
  // i.e. `Page 1`, `Page 2 from end`, `Last Page`

  var accessiblePageString = function accessiblePageString() {
    if (activePage < -1) return "".concat(pageLabel, " ").concat(Math.abs(activePage), " ").concat(fromEndLabel);
    if (activePage === -1) return "".concat(lastLabel, " ").concat(pageLabel);
    return "".concat(pageLabel, " ").concat(activePage + 1);
  }; // If `pageCount` is unknown call it `collection`


  var accessibleCollectionString = pageCount === 0 ? collectionLabel : pageCount.toString(); // Create the whole string with total pageCount or `collection`

  var accessiblePageCount = "".concat(accessiblePageString(), " ").concat(ofLabel, " ").concat(accessibleCollectionString);
  return ___EmotionJSX("nav", _extends({
    className: classes
  }, rest), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", {
    "aria-atomic": "true",
    "aria-relevant": "additions text",
    role: "status"
  }, accessiblePageCount)), firstButton, previousButton, centerPageCount, nextButton, lastButton);
};

var PaginationButtonWrapper = function PaginationButtonWrapper(_ref2) {
  var pageIndex = _ref2.pageIndex,
      _ref2$inList = _ref2.inList,
      inList = _ref2$inList === void 0 ? true : _ref2$inList,
      activePage = _ref2.activePage,
      pageCount = _ref2.pageCount,
      ariaControls = _ref2.ariaControls,
      safeClick = _ref2.safeClick,
      disabled = _ref2.disabled;

  var button = ___EmotionJSX(EuiPaginationButton, {
    isActive: pageIndex === activePage,
    totalPages: pageCount,
    onClick: function onClick(e) {
      return safeClick(e, pageIndex);
    },
    pageIndex: pageIndex,
    "aria-controls": ariaControls,
    disabled: disabled
  });

  if (inList) {
    return ___EmotionJSX("li", {
      className: "euiPagination__item"
    }, button);
  }

  return button;
};