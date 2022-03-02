function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _unsupportedIterableToArray(arr) || _nonIterableSpread(); }

function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _iterableToArray(iter) { if (typeof Symbol !== "undefined" && Symbol.iterator in Object(iter)) return Array.from(iter); }

function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) return _arrayLikeToArray(arr); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { Comparators } from './comparators';

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
export var SortableProperties = /*#__PURE__*/function () {
  /**
   * @param {Array<SortableProperty>} sortableProperties - a set of sortable properties.
   * @param {string} initialSortablePropertyName - Which sort property should be sorted on by default.
   */
  function SortableProperties(sortableProperties, initialSortablePropertyName) {
    _classCallCheck(this, SortableProperties);

    _defineProperty(this, "sortableProperties", void 0);

    _defineProperty(this, "currentSortedProperty", void 0);

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


  _createClass(SortableProperties, [{
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
      var copy = _toConsumableArray(items);

      var comparator = Comparators.value(this.getSortedProperty().getValue);

      if (!this.isCurrentSortAscending()) {
        comparator = Comparators.reverse(comparator);
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