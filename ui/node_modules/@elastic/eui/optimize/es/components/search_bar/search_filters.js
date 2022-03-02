import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Component, Fragment } from 'react';
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