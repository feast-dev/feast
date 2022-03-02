function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

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
import React, { Component } from 'react';
import PropTypes from "prop-types";
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

EuiSelectableSearch.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Passes back (matchingOptions, searchValue)
       */
  onChange: PropTypes.func.isRequired,
  options: PropTypes.arrayOf(PropTypes.shape({
    /**
       * Optional `boolean`.
       * Set to `true` to indicate object is just a grouping label, not a selectable item
       */
    isGroupLabel: PropTypes.oneOfType([PropTypes.oneOf([true]).isRequired, PropTypes.oneOf([false])]),
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,

    /**
       * Visible label of option.
       * Must be unique across items if `key` is not supplied
       */
    label: PropTypes.string,

    /**
       * Optionally change the searchable term by passing a different string other than the `label`.
       * Best used when creating a custom `optionRender` to separate the label from metadata but allowing to search on both
       */
    searchableLabel: PropTypes.string,

    /**
       * Must be unique across items.
       * Will be used to match options instead of `label`
       */
    key: PropTypes.string,

    /**
       * Leave `undefined` to indicate not selected,
       * 'on' to indicate inclusion and
       * 'off' to indicate exclusion
       */
    checked: PropTypes.oneOf(["on", "off", undefined]),
    disabled: PropTypes.bool,

    /**
       * Node to add between the selection icon and the label
       */
    prepend: PropTypes.node,

    /**
       * Node to add to the far right of the item
       */
    append: PropTypes.node,
    ref: PropTypes.func,

    /**
       * Option data to pass through to the `renderOptions` element.
       * Bypass `EuiSelectableItem` and avoid DOM attribute warnings.
       */
    data: PropTypes.shape({})
  }).isRequired).isRequired,
  defaultValue: PropTypes.string.isRequired,

  /**
       * The id of the visible list to create the appropriate aria controls
       */
  listId: PropTypes.string,
  isPreFiltered: PropTypes.bool.isRequired
};