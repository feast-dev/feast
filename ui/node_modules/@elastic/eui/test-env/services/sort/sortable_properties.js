"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.SortableProperties = void 0;

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _comparators = require("./comparators");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * @typedef {Object} SortableProperty
 * @property {string} sortableProperty.name - Name of the property.
 * @property {function} sortableProperty.getValue - A function that takes in an object and returns a value to sort
 * by.
 * @property {boolean} sortableProperty.isAscending - The direction of the last sort by this property. Used to preserve
 * past sort orders.
 */

/**
 * Stores sort information for a set of SortableProperties, including which property is currently being sorted on, as
 * well as the last sort order for each property.
 */
var SortableProperties = /*#__PURE__*/function () {
  /**
   * @param {Array<SortableProperty>} sortableProperties - a set of sortable properties.
   * @param {string} initialSortablePropertyName - Which sort property should be sorted on by default.
   */
  function SortableProperties(sortableProperties, initialSortablePropertyName) {
    (0, _classCallCheck2.default)(this, SortableProperties);
    (0, _defineProperty2.default)(this, "sortableProperties", void 0);
    (0, _defineProperty2.default)(this, "currentSortedProperty", void 0);
    this.sortableProperties = sortableProperties;
    /**
     * The current property that is being sorted on.
     * @type {SortableProperty}
     */

    var currentSortedProperty = this.getSortablePropertyByName(initialSortablePropertyName);

    if (!currentSortedProperty) {
      throw new Error("No property with the name ".concat(initialSortablePropertyName));
    }

    this.currentSortedProperty = currentSortedProperty;
  }
  /**
   * @returns {SortableProperty} The current property that is being sorted on. Undefined if no sort order is applied.
   */


  (0, _createClass2.default)(SortableProperties, [{
    key: "getSortedProperty",
    value: function getSortedProperty() {
      return this.currentSortedProperty;
    }
    /**
     * Sorts the items passed in and returns a newly sorted array.
     * @param items {Array.<Object>}
     * @returns {Array.<Object>} sorted array of items, based off the sort properties.
     */

  }, {
    key: "sortItems",
    value: function sortItems(items) {
      var copy = (0, _toConsumableArray2.default)(items);

      var comparator = _comparators.Comparators.value(this.getSortedProperty().getValue);

      if (!this.isCurrentSortAscending()) {
        comparator = _comparators.Comparators.reverse(comparator);
      }

      copy.sort(comparator);
      return copy;
    }
    /**
     * Returns the SortProperty with the given name, if found.
     * @param {String} propertyName
     * @returns {SortableProperty|undefined}
     */

  }, {
    key: "getSortablePropertyByName",
    value: function getSortablePropertyByName(propertyName) {
      return this.sortableProperties.find(function (property) {
        return property.name === propertyName;
      });
    }
    /**
     * Updates the sort property, potentially flipping the sort order based on whether the same
     * property was already being sorted.
     * @param propertyName {String}
     */

  }, {
    key: "sortOn",
    value: function sortOn(propertyName) {
      var newSortedProperty = this.getSortablePropertyByName(propertyName);

      if (!newSortedProperty) {
        throw new Error("No property with the name ".concat(propertyName));
      }

      var sortedProperty = this.getSortedProperty();

      if (sortedProperty.name === newSortedProperty.name) {
        this.flipCurrentSortOrder();
      } else {
        this.currentSortedProperty = newSortedProperty;
      }
    }
    /**
     * @returns {boolean} True if the current sortable property is sorted in ascending order.
     */

  }, {
    key: "isCurrentSortAscending",
    value: function isCurrentSortAscending() {
      var sortedProperty = this.getSortedProperty();
      return sortedProperty ? this.isAscendingByName(sortedProperty.name) : false;
    }
    /**
     * @param {string} propertyName
     * @returns {boolean} True if the given sort property is sorted in ascending order.
     */

  }, {
    key: "isAscendingByName",
    value: function isAscendingByName(propertyName) {
      var sortedProperty = this.getSortablePropertyByName(propertyName);
      return sortedProperty ? sortedProperty.isAscending : false;
    }
    /**
     * Flips the current sorted property sort order.
     */

  }, {
    key: "flipCurrentSortOrder",
    value: function flipCurrentSortOrder() {
      this.currentSortedProperty.isAscending = !this.currentSortedProperty.isAscending;
    }
  }]);
  return SortableProperties;
}();

exports.SortableProperties = SortableProperties;