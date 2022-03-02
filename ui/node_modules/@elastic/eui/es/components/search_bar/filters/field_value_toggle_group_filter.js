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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Component } from 'react';
import PropTypes from "prop-types";
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
FieldValueToggleGroupFilter.propTypes = {
  index: PropTypes.number.isRequired,
  config: PropTypes.shape({
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
  }).isRequired,
  query: PropTypes.any.isRequired,
  onChange: PropTypes.func.isRequired
};