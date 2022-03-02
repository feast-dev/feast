import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Component } from 'react';
import { EuiFilterButton } from '../../filter_group';
import { Query } from '../query';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var FieldValueToggleGroupFilter = /*#__PURE__*/function (_Component) {
  _inherits(FieldValueToggleGroupFilter, _Component);

  var _super = _createSuper(FieldValueToggleGroupFilter);

  function FieldValueToggleGroupFilter() {
    _classCallCheck(this, FieldValueToggleGroupFilter);

    return _super.apply(this, arguments);
  }

  _createClass(FieldValueToggleGroupFilter, [{
    key: "resolveDisplay",
    value: function resolveDisplay(config, query, item) {
      var clause = query.getSimpleFieldClause(config.field, item.value);

      if (clause) {
        if (Query.isMust(clause)) {
          return {
            active: true,
            name: item.name
          };
        }

        return {
          active: true,
          name: item.negatedName ? item.negatedName : "Not ".concat(item.name)
        };
      }

      return {
        active: false,
        name: item.name
      };
    }
  }, {
    key: "valueChanged",
    value: function valueChanged(item, active) {
      var field = this.props.config.field;
      var value = item.value,
          operator = item.operator;
      var query = active ? this.props.query.removeSimpleFieldClauses(field) : this.props.query.removeSimpleFieldClauses(field).addSimpleFieldValue(field, value, true, operator);
      this.props.onChange(query);
    }
  }, {
    key: "render",
    value: function render() {
      var _this = this;

      var _this$props = this.props,
          config = _this$props.config,
          query = _this$props.query;
      return config.items.map(function (item, index) {
        var _this$resolveDisplay = _this.resolveDisplay(config, query, item),
            active = _this$resolveDisplay.active,
            name = _this$resolveDisplay.name;

        var onClick = function onClick() {
          _this.valueChanged(item, active);
        };

        var key = "field_value_toggle_filter_item_".concat(index);
        var isLastItem = index === config.items.length - 1;
        return ___EmotionJSX(EuiFilterButton, {
          key: key,
          onClick: onClick,
          hasActiveFilters: active,
          noDivider: !isLastItem,
          "aria-pressed": !!active,
          withNext: !isLastItem
        }, name);
      });
    }
  }]);

  return FieldValueToggleGroupFilter;
}(Component);