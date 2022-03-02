"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Pager = void 0;

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _predicate = require("../predicate");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var Pager = function Pager(_totalItems, _itemsPerPage) {
  var _this = this;

  var initialPageIndex = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 0;
  (0, _classCallCheck2.default)(this, Pager);
  (0, _defineProperty2.default)(this, "currentPageIndex", void 0);
  (0, _defineProperty2.default)(this, "firstItemIndex", void 0);
  (0, _defineProperty2.default)(this, "itemsPerPage", void 0);
  (0, _defineProperty2.default)(this, "lastItemIndex", void 0);
  (0, _defineProperty2.default)(this, "totalItems", void 0);
  (0, _defineProperty2.default)(this, "totalPages", void 0);
  (0, _defineProperty2.default)(this, "setTotalItems", function (totalItems) {
    _this.totalItems = totalItems;

    _this.update();
  });
  (0, _defineProperty2.default)(this, "setItemsPerPage", function (itemsPerPage) {
    _this.itemsPerPage = itemsPerPage;

    _this.update();
  });
  (0, _defineProperty2.default)(this, "isPageable", function () {
    return _this.firstItemIndex !== -1;
  });
  (0, _defineProperty2.default)(this, "getTotalPages", function () {
    return _this.totalPages;
  });
  (0, _defineProperty2.default)(this, "getCurrentPageIndex", function () {
    return _this.currentPageIndex;
  });
  (0, _defineProperty2.default)(this, "getFirstItemIndex", function () {
    return _this.firstItemIndex;
  });
  (0, _defineProperty2.default)(this, "getLastItemIndex", function () {
    return _this.lastItemIndex;
  });
  (0, _defineProperty2.default)(this, "hasNextPage", function () {
    return _this.currentPageIndex < _this.totalPages - 1;
  });
  (0, _defineProperty2.default)(this, "hasPreviousPage", function () {
    return _this.currentPageIndex > 0;
  });
  (0, _defineProperty2.default)(this, "goToNextPage", function () {
    _this.goToPageIndex(_this.currentPageIndex + 1);
  });
  (0, _defineProperty2.default)(this, "goToPreviousPage", function () {
    _this.goToPageIndex(_this.currentPageIndex - 1);
  });
  (0, _defineProperty2.default)(this, "goToPageIndex", function (pageIndex) {
    _this.currentPageIndex = pageIndex;

    _this.update();
  });
  (0, _defineProperty2.default)(this, "update", function () {
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

  if (!(0, _predicate.isNumber)(_totalItems) || isNaN(_totalItems)) {
    throw new Error('Please provide a number of totalItems');
  }

  if (!(0, _predicate.isNumber)(_itemsPerPage) || isNaN(_itemsPerPage)) {
    throw new Error('Please provide a number of itemsPerPage');
  }

  if (!(0, _predicate.isNumber)(initialPageIndex) || isNaN(initialPageIndex)) {
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

exports.Pager = Pager;