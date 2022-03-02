import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
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
import React, { Component } from 'react';
import classNames from 'classnames';
import { EuiFieldSearch } from '../../form';
import { getMatchingOptions } from '../matching_options';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiSelectableSearch = /*#__PURE__*/function (_Component) {
  _inherits(EuiSelectableSearch, _Component);

  var _super = _createSuper(EuiSelectableSearch);

  function EuiSelectableSearch(props) {
    var _this;

    _classCallCheck(this, EuiSelectableSearch);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "onSearchChange", function (value) {
      if (value !== _this.state.searchValue) {
        _this.setState({
          searchValue: value
        }, function () {
          var matchingOptions = getMatchingOptions(_this.props.options, value, _this.props.isPreFiltered);

          _this.props.onChange(matchingOptions, value);
        });
      }
    });

    _this.state = {
      searchValue: props.defaultValue
    };
    return _this;
  }

  _createClass(EuiSelectableSearch, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      var searchValue = this.state.searchValue;
      var matchingOptions = getMatchingOptions(this.props.options, searchValue, this.props.isPreFiltered);
      this.props.onChange(matchingOptions, searchValue);
    }
  }, {
    key: "render",
    value: function render() {
      var _this$props = this.props,
          className = _this$props.className,
          onChange = _this$props.onChange,
          options = _this$props.options,
          defaultValue = _this$props.defaultValue,
          listId = _this$props.listId,
          placeholder = _this$props.placeholder,
          isPreFiltered = _this$props.isPreFiltered,
          rest = _objectWithoutProperties(_this$props, ["className", "onChange", "options", "defaultValue", "listId", "placeholder", "isPreFiltered"]);

      var classes = classNames('euiSelectableSearch', className);
      var ariaPropsIfListIsPresent = listId ? {
        role: 'combobox',
        'aria-autocomplete': 'list',
        'aria-expanded': true,
        'aria-controls': listId,
        'aria-owns': listId // legacy attribute but shims support for nearly everything atm

      } : undefined;
      return ___EmotionJSX(EuiFieldSearch, _extends({
        className: classes,
        placeholder: placeholder,
        onSearch: this.onSearchChange,
        incremental: true,
        defaultValue: defaultValue,
        fullWidth: true,
        autoComplete: "off",
        "aria-haspopup": "listbox"
      }, ariaPropsIfListIsPresent, rest));
    }
  }]);

  return EuiSelectableSearch;
}(Component);

_defineProperty(EuiSelectableSearch, "defaultProps", {
  defaultValue: ''
});