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