function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function"); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, writable: true, configurable: true } }); if (superClass) _setPrototypeOf(subClass, superClass); }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _possibleConstructorReturn(self, call) { if (call && (_typeof(call) === "object" || typeof call === "function")) { return call; } return _assertThisInitialized(self); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Component, Fragment } from 'react';
import PropTypes from "prop-types";
import { createFilter } from './filters';
import { EuiFilterGroup } from '../filter_group';
import { jsx as ___EmotionJSX } from "@emotion/react";
export {} from './filters';
export var EuiSearchFilters = /*#__PURE__*/function (_Component) {
  _inherits(EuiSearchFilters, _Component);

  var _super = _createSuper(EuiSearchFilters);

  function EuiSearchFilters() {
    _classCallCheck(this, EuiSearchFilters);

    return _super.apply(this, arguments);
  }

  _createClass(EuiSearchFilters, [{
    key: "render",
    value: function render() {
      var _this$props = this.props,
          _this$props$filters = _this$props.filters,
          filters = _this$props$filters === void 0 ? [] : _this$props$filters,
          query = _this$props.query,
          onChange = _this$props.onChange;
      var items = [];
      filters.forEach(function (filterConfig, index) {
        if (filterConfig.available && !filterConfig.available()) {
          return;
        }

        var key = "filter_".concat(index);
        var control = createFilter(index, filterConfig, query, onChange);
        items.push(___EmotionJSX(Fragment, {
          key: key
        }, control));
      });
      return ___EmotionJSX(EuiFilterGroup, null, items);
    }
  }]);

  return EuiSearchFilters;
}(Component);

_defineProperty(EuiSearchFilters, "defaultProps", {
  filters: []
});

EuiSearchFilters.propTypes = {
  query: PropTypes.any.isRequired,
  onChange: PropTypes.func.isRequired,
  filters: PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.shape({
    type: PropTypes.oneOf(["is"]).isRequired,
    field: PropTypes.string.isRequired,
    name: PropTypes.string.isRequired,
    negatedName: PropTypes.string,
    available: PropTypes.func
  }).isRequired, PropTypes.shape({
    type: PropTypes.oneOf(["field_value_selection"]).isRequired,
    field: PropTypes.string,
    name: PropTypes.string.isRequired,

    /**
       * See #FieldValueOptionType
       */
    options: PropTypes.oneOfType([PropTypes.arrayOf(PropTypes.shape({
      field: PropTypes.string,
      value: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired, PropTypes.bool.isRequired, PropTypes.shape({
        type: PropTypes.oneOf(["date"]).isRequired,
        raw: PropTypes.any.isRequired,
        granularity: PropTypes.oneOfType([PropTypes.shape({
          es: PropTypes.oneOf(["d", "w", "M", "y"]).isRequired,
          js: PropTypes.oneOf(["day", "week", "month", "year"]).isRequired,
          isSame: PropTypes.func.isRequired,
          start: PropTypes.func.isRequired,
          startOfNext: PropTypes.func.isRequired,
          iso8601: PropTypes.func.isRequired
        }).isRequired, PropTypes.oneOf([undefined])]).isRequired,
        text: PropTypes.string.isRequired,
        resolve: PropTypes.func.isRequired
      }).isRequired]).isRequired,
      name: PropTypes.string,
      view: PropTypes.node
    }).isRequired).isRequired, PropTypes.func.isRequired]).isRequired,
    filterWith: PropTypes.oneOfType([PropTypes.oneOf(["prefix", "includes"]), PropTypes.func.isRequired]),
    cache: PropTypes.number,
    multiSelect: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.oneOf(["and", "or"])]),
    loadingMessage: PropTypes.string,
    noOptionsMessage: PropTypes.string,
    searchThreshold: PropTypes.number,
    available: PropTypes.func,
    autoClose: PropTypes.bool,
    operator: PropTypes.oneOf(["eq", "exact", "gt", "gte", "lt", "lte"])
  }).isRequired, PropTypes.shape({
    type: PropTypes.oneOf(["field_value_toggle"]).isRequired,
    field: PropTypes.string.isRequired,
    value: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired, PropTypes.bool.isRequired, PropTypes.shape({
      type: PropTypes.oneOf(["date"]).isRequired,
      raw: PropTypes.any.isRequired,
      granularity: PropTypes.oneOfType([PropTypes.shape({
        es: PropTypes.oneOf(["d", "w", "M", "y"]).isRequired,
        js: PropTypes.oneOf(["day", "week", "month", "year"]).isRequired,
        isSame: PropTypes.func.isRequired,
        start: PropTypes.func.isRequired,
        startOfNext: PropTypes.func.isRequired,
        iso8601: PropTypes.func.isRequired
      }).isRequired, PropTypes.oneOf([undefined])]).isRequired,
      text: PropTypes.string.isRequired,
      resolve: PropTypes.func.isRequired
    }).isRequired]).isRequired,
    name: PropTypes.string.isRequired,
    negatedName: PropTypes.string,
    available: PropTypes.func,
    operator: PropTypes.oneOf(["eq", "exact", "gt", "gte", "lt", "lte"])
  }).isRequired, PropTypes.shape({
    type: PropTypes.oneOf(["field_value_toggle_group"]).isRequired,
    field: PropTypes.string.isRequired,

    /**
       * See #FieldValueToggleGroupFilterItemType
       */
    items: PropTypes.arrayOf(PropTypes.shape({
      value: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired, PropTypes.bool.isRequired]).isRequired,
      name: PropTypes.string.isRequired,
      negatedName: PropTypes.string,
      operator: PropTypes.oneOf(["eq", "exact", "gt", "gte", "lt", "lte"])
    }).isRequired).isRequired,
    available: PropTypes.func
  }).isRequired]).isRequired).isRequired
};