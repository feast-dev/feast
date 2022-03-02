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
import { Browser } from '../../../services/browser';
import { keys } from '../../../services';
import { EuiFormControlLayout } from '../form_control_layout';
import { EuiValidatableControl } from '../validatable_control';
import { jsx as ___EmotionJSX } from "@emotion/react";
var isSearchSupported = false;
export var EuiFieldSearch = /*#__PURE__*/function (_Component) {
  _inherits(EuiFieldSearch, _Component);

  var _super = _createSuper(EuiFieldSearch);

  function EuiFieldSearch() {
    var _this;

    _classCallCheck(this, EuiFieldSearch);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "state", {
      value: _this.props.value || (_this.props.defaultValue ? "".concat(_this.props.defaultValue) : '')
    });

    _defineProperty(_assertThisInitialized(_this), "inputElement", null);

    _defineProperty(_assertThisInitialized(_this), "cleanups", []);

    _defineProperty(_assertThisInitialized(_this), "onClear", function () {
      // clear the field's value
      // 1. React doesn't listen for `change` events, instead it maps `input` events to `change`
      // 2. React only fires the mapped `change` event if the element's value has changed
      // 3. An input's value is, in addition to other methods, tracked by intercepting element.value = '...'
      //
      // So we have to go below the element's value setter to avoid React intercepting it,
      // only then will React treat the value as different and fire its `change` event
      //
      // https://stackoverflow.com/questions/23892547/what-is-the-best-way-to-trigger-onchange-event-in-react-js
      var nativeInputValue = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
      var nativeInputValueSetter = nativeInputValue ? nativeInputValue.set : undefined;

      if (nativeInputValueSetter) {
        nativeInputValueSetter.call(_this.inputElement, '');
      } // dispatch input event, with IE11 support/fallback


      var event;

      if ('Event' in window && typeof Event === 'function') {
        event = new Event('input', {
          bubbles: true,
          cancelable: false
        });
      } else {
        // IE11
        event = document.createEvent('Event');
        event.initEvent('input', true, false);
      }

      if (_this.inputElement) {
        if (event) {
          _this.inputElement.dispatchEvent(event);
        } // set focus on the search field


        _this.inputElement.focus();

        _this.inputElement.dispatchEvent(new Event('change'));
      }

      _this.setState({
        value: ''
      });

      var _this$props = _this.props,
          incremental = _this$props.incremental,
          onSearch = _this$props.onSearch;

      if (onSearch && incremental) {
        onSearch('');
      }
    });

    _defineProperty(_assertThisInitialized(_this), "setRef", function (inputElement) {
      _this.inputElement = inputElement;

      if (_this.props.inputRef) {
        _this.props.inputRef(inputElement);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onKeyUp", function (event, incremental, onSearch) {
      _this.setState({
        value: event.target.value
      });

      if (_this.props.onKeyUp) {
        _this.props.onKeyUp(event);

        if (event.defaultPrevented) {
          return;
        }
      }

      if (onSearch && (event.key !== keys.ENTER && incremental || event.key === keys.ENTER && !isSearchSupported)) {
        onSearch(event.target.value);
      }
    });

    return _this;
  }

  _createClass(EuiFieldSearch, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      var _this2 = this;

      if (!this.inputElement) return;
      isSearchSupported = Browser.isEventSupported('search', this.inputElement);

      if (isSearchSupported) {
        var onSearch = function onSearch(event) {
          if (_this2.props.onSearch) {
            if (!event || !event.target || event.defaultPrevented) return;

            _this2.props.onSearch(event.target.value);
          }
        };

        this.inputElement.addEventListener('search', onSearch);
        this.cleanups.push(function () {
          if (!_this2.inputElement) return;

          _this2.inputElement.removeEventListener('search', onSearch);
        });
      }

      var onChange = function onChange(event) {
        if (event.target && event.target.value !== _this2.state.value) {
          _this2.setState({
            value: event.target.value
          });

          if (_this2.props.onSearch) {
            _this2.props.onSearch(event.target.value);
          }
        }
      };

      this.inputElement.addEventListener('change', onChange);
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this.cleanups.forEach(function (cleanup) {
        return cleanup();
      });
    }
  }, {
    key: "render",
    value: function render() {
      var _this3 = this;

      var _this$props2 = this.props,
          className = _this$props2.className,
          id = _this$props2.id,
          name = _this$props2.name,
          placeholder = _this$props2.placeholder,
          isInvalid = _this$props2.isInvalid,
          fullWidth = _this$props2.fullWidth,
          isLoading = _this$props2.isLoading,
          inputRef = _this$props2.inputRef,
          incremental = _this$props2.incremental,
          compressed = _this$props2.compressed,
          onSearch = _this$props2.onSearch,
          isClearable = _this$props2.isClearable,
          append = _this$props2.append,
          prepend = _this$props2.prepend,
          rest = _objectWithoutProperties(_this$props2, ["className", "id", "name", "placeholder", "isInvalid", "fullWidth", "isLoading", "inputRef", "incremental", "compressed", "onSearch", "isClearable", "append", "prepend"]);

      var value = this.props.value;
      if (typeof this.props.value !== 'string') value = this.state.value;
      var classes = classNames('euiFieldSearch', {
        'euiFieldSearch--fullWidth': fullWidth,
        'euiFieldSearch--compressed': compressed,
        'euiFieldSearch--inGroup': prepend || append,
        'euiFieldSearch-isLoading': isLoading,
        'euiFieldSearch-isClearable': isClearable && value
      }, className);
      return ___EmotionJSX(EuiFormControlLayout, {
        icon: "search",
        fullWidth: fullWidth,
        isLoading: isLoading,
        clear: isClearable && value && !rest.readOnly && !rest.disabled ? {
          onClick: this.onClear
        } : undefined,
        compressed: compressed,
        append: append,
        prepend: prepend
      }, ___EmotionJSX(EuiValidatableControl, {
        isInvalid: isInvalid
      }, ___EmotionJSX("input", _extends({
        type: "search",
        id: id,
        name: name,
        placeholder: placeholder,
        className: classes,
        onKeyUp: function onKeyUp(e) {
          return _this3.onKeyUp(e, incremental, onSearch);
        },
        ref: this.setRef
      }, rest))));
    }
  }]);

  return EuiFieldSearch;
}(Component);

_defineProperty(EuiFieldSearch, "defaultProps", {
  fullWidth: false,
  isLoading: false,
  incremental: false,
  compressed: false,
  isClearable: true
});

EuiFieldSearch.propTypes = {
  name: PropTypes.string,
  id: PropTypes.string,
  placeholder: PropTypes.string,
  value: PropTypes.string,
  isInvalid: PropTypes.bool,
  fullWidth: PropTypes.bool,
  isLoading: PropTypes.bool,

  /**
     * Called when the user presses [Enter] OR on change if the incremental prop is `true`.
     * If you don't need the on[Enter] functionality, prefer using onChange
     */
  onSearch: PropTypes.func,

  /**
     * When `true` the search will be executed (that is, the `onSearch` will be called) as the
     * user types.
     */
  incremental: PropTypes.bool,

  /**
     * when `true` creates a shorter height input
     */
  compressed: PropTypes.bool,
  inputRef: PropTypes.func,

  /**
     * Shows a button that quickly clears any input
     */
  isClearable: PropTypes.bool,

  /**
     * Creates an input group with element(s) coming before input
     * `string` | `ReactElement` or an array of these
     */
  prepend: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),

  /**
     * Creates an input group with element(s) coming after input.
     * `string` | `ReactElement` or an array of these
     */
  append: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};