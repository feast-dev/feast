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
import { isNil } from '../../../services/predicate';
import { Query } from '../query';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var IsFilter = /*#__PURE__*/function (_Component) {
  _inherits(IsFilter, _Component);

  var _super = _createSuper(IsFilter);

  function IsFilter() {
    _classCallCheck(this, IsFilter);

    return _super.apply(this, arguments);
  }

  _createClass(IsFilter, [{
    key: "resolveDisplay",
    value: function resolveDisplay(clause) {
      var _this$props$config = this.props.config,
          name = _this$props$config.name,
          negatedName = _this$props$config.negatedName;

      if (isNil(clause)) {
        return {
          hasActiveFilters: false,
          name: name
        };
      }

      return Query.isMust(clause) ? {
        hasActiveFilters: true,
        name: name
      } : {
        hasActiveFilters: true,
        name: negatedName ? negatedName : "Not ".concat(name)
      };
    }
  }, {
    key: "valueChanged",
    value: function valueChanged(field, checked) {
      var query = checked ? this.props.query.removeIsClause(field) : this.props.query.addMustIsClause(field);
      this.props.onChange(query);
    }
  }, {
    key: "render",
    value: function render() {
      var _this = this;

      var _this$props = this.props,
          query = _this$props.query,
          config = _this$props.config;
      var clause = query.getIsClause(config.field);
      var checked = !isNil(clause);

      var _this$resolveDisplay = this.resolveDisplay(clause),
          hasActiveFilters = _this$resolveDisplay.hasActiveFilters,
          name = _this$resolveDisplay.name;

      var onClick = function onClick() {
        _this.valueChanged(config.field, checked);
      };

      return ___EmotionJSX(EuiFilterButton, {
        onClick: onClick,
        hasActiveFilters: hasActiveFilters,
        "aria-pressed": !!hasActiveFilters
      }, name);
    }
  }]);

  return IsFilter;
}(Component);