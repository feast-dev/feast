function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { isNumber } from '../predicate';
export var Pager = function Pager(_totalItems, _itemsPerPage) {
  var _this = this;

  var initialPageIndex = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 0;

  _classCallCheck(this, Pager);

  _defineProperty(this, "currentPageIndex", void 0);

  _defineProperty(this, "firstItemIndex", void 0);

  _defineProperty(this, "itemsPerPage", void 0);

  _defineProperty(this, "lastItemIndex", void 0);

  _defineProperty(this, "totalItems", void 0);

  _defineProperty(this, "totalPages", void 0);

  _defineProperty(this, "setTotalItems", function (totalItems) {
    _this.totalItems = totalItems;

    _this.update();
  });

  _defineProperty(this, "setItemsPerPage", function (itemsPerPage) {
    _this.itemsPerPage = itemsPerPage;

    _this.update();
  });

  _defineProperty(this, "isPageable", function () {
    return _this.firstItemIndex !== -1;
  });

  _defineProperty(this, "getTotalPages", function () {
    return _this.totalPages;
  });

  _defineProperty(this, "getCurrentPageIndex", function () {
    return _this.currentPageIndex;
  });

  _defineProperty(this, "getFirstItemIndex", function () {
    return _this.firstItemIndex;
  });

  _defineProperty(this, "getLastItemIndex", function () {
    return _this.lastItemIndex;
  });

  _defineProperty(this, "hasNextPage", function () {
    return _this.currentPageIndex < _this.totalPages - 1;
  });

  _defineProperty(this, "hasPreviousPage", function () {
    return _this.currentPageIndex > 0;
  });

  _defineProperty(this, "goToNextPage", function () {
    _this.goToPageIndex(_this.currentPageIndex + 1);
  });

  _defineProperty(this, "goToPreviousPage", function () {
    _this.goToPageIndex(_this.currentPageIndex - 1);
  });

  _defineProperty(this, "goToPageIndex", function (pageIndex) {
    _this.currentPageIndex = pageIndex;

    _this.update();
  });

  _defineProperty(this, "update", function () {
    if (_this.totalItems <= 0) {
      _this.totalPages = 0;
      _this.currentPageIndex = 0;
      _this.firstItemIndex = -1;
      _this.lastItemIndex = -1;
      return;
    }

    _this.totalPages = Math.ceil(_this.totalItems / _this.itemsPerPage); // Ensure the current page falls within our range of total pages.

    _this.currentPageIndex = Math.min(Math.max(0, _this.currentPageIndex), _this.totalPages - 1); // Find the range of visible items on the current page.

    _this.firstItemIndex = _this.currentPageIndex * _this.itemsPerPage;
    _this.lastItemIndex = Math.min(_this.firstItemIndex + _this.itemsPerPage, _this.totalItems) - 1;
  });

  if (!isNumber(_totalItems) || isNaN(_totalItems)) {
    throw new Error('Please provide a number of totalItems');
  }

  if (!isNumber(_itemsPerPage) || isNaN(_itemsPerPage)) {
    throw new Error('Please provide a number of itemsPerPage');
  }

  if (!isNumber(initialPageIndex) || isNaN(initialPageIndex)) {
    throw new Error('Please provide a number of initialPageIndex');
  }

  this.currentPageIndex = initialPageIndex;
  this.firstItemIndex = -1;
  this.itemsPerPage = _itemsPerPage;
  this.lastItemIndex = -1;
  this.totalItems = _totalItems;
  this.totalPages = 0;
  this.update();
};