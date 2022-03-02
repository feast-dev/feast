function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

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
import React, { Component } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import AutosizeInput from 'react-input-autosize';
import { EuiScreenReaderOnly } from '../../accessibility';
import { EuiFormControlLayout } from '../../form/form_control_layout';
import { EuiComboBoxPill } from './combo_box_pill';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiComboBoxInput = /*#__PURE__*/function (_Component) {
  _inherits(EuiComboBoxInput, _Component);

  var _super = _createSuper(EuiComboBoxInput);

  function EuiComboBoxInput() {
    var _this;

    _classCallCheck(this, EuiComboBoxInput);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "state", {
      hasFocus: false
    });

    _defineProperty(_assertThisInitialized(_this), "updatePosition", function () {
      // Wait a beat for the DOM to update, since we depend on DOM elements' bounds.
      requestAnimationFrame(function () {
        _this.props.updatePosition();
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onFocus", function (event) {
      _this.props.onFocus(event);

      _this.setState({
        hasFocus: true
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onBlur", function (event) {
      if (_this.props.onBlur) {
        _this.props.onBlur(event);
      }

      _this.setState({
        hasFocus: false
      });
    });

    _defineProperty(_assertThisInitialized(_this), "inputOnChange", function (event) {
      var _this$props = _this.props,
          onChange = _this$props.onChange,
          searchValue = _this$props.searchValue;

      if (onChange) {
        onChange(event.target.value);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "inputRefCallback", function (ref) {
      var autoSizeInputRef = _this.props.autoSizeInputRef;

      if (autoSizeInputRef) {
        autoSizeInputRef(ref);
      }
    });

    return _this;
  }

  _createClass(EuiComboBoxInput, [{
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      var searchValue = prevProps.searchValue; // We need to update the position of everything if the user enters enough input to change
      // the size of the input.

      if (searchValue !== this.props.searchValue) {
        this.updatePosition();
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this$props2 = this.props,
          compressed = _this$props2.compressed,
          focusedOptionId = _this$props2.focusedOptionId,
          fullWidth = _this$props2.fullWidth,
          hasSelectedOptions = _this$props2.hasSelectedOptions,
          id = _this$props2.id,
          inputRef = _this$props2.inputRef,
          isDisabled = _this$props2.isDisabled,
          isListOpen = _this$props2.isListOpen,
          noIcon = _this$props2.noIcon,
          onClear = _this$props2.onClear,
          onClick = _this$props2.onClick,
          onCloseListClick = _this$props2.onCloseListClick,
          onOpenListClick = _this$props2.onOpenListClick,
          onRemoveOption = _this$props2.onRemoveOption,
          placeholder = _this$props2.placeholder,
          rootId = _this$props2.rootId,
          searchValue = _this$props2.searchValue,
          selectedOptions = _this$props2.selectedOptions,
          singleSelectionProp = _this$props2.singleSelection,
          toggleButtonRef = _this$props2.toggleButtonRef,
          value = _this$props2.value,
          prepend = _this$props2.prepend,
          append = _this$props2.append,
          isLoading = _this$props2.isLoading,
          autoFocus = _this$props2.autoFocus,
          ariaLabel = _this$props2['aria-label'],
          ariaLabelledby = _this$props2['aria-labelledby'];
      var singleSelection = Boolean(singleSelectionProp);
      var asPlainText = singleSelectionProp && _typeof(singleSelectionProp) === 'object' && singleSelectionProp.asPlainText || false;
      var pills = selectedOptions ? selectedOptions.map(function (option) {
        var key = option.key,
            label = option.label,
            color = option.color,
            onClick = option.onClick,
            rest = _objectWithoutProperties(option, ["key", "label", "color", "onClick"]);

        var pillOnClose = isDisabled || singleSelection || onClick ? undefined : onRemoveOption;
        return ___EmotionJSX(EuiComboBoxPill, _extends({
          option: option,
          onClose: pillOnClose,
          key: key !== null && key !== void 0 ? key : label.toLowerCase(),
          color: color,
          onClick: onClick,
          onClickAriaLabel: onClick ? 'Change' : undefined,
          asPlainText: asPlainText
        }, rest), label);
      }) : null;
      var removeOptionMessage;
      var removeOptionMessageId;

      if (this.state.hasFocus) {
        var readPlaceholder = placeholder ? "".concat(placeholder, ".") : '';
        var removeOptionMessageContent = "Combo box. Selected. ".concat(searchValue ? "".concat(searchValue, ". Selected. ") : '').concat(selectedOptions && selectedOptions.length > 0 ? "".concat(value, ". Press Backspace to delete ").concat(selectedOptions[selectedOptions.length - 1].label, ". ") : '', "Combo box input. ").concat(readPlaceholder, " Type some text or, to display a list of choices, press Down Arrow. ") + 'To exit the list of choices, press Escape.';
        removeOptionMessageId = rootId('removeOptionMessage'); // aria-live="assertive" will read this message aloud immediately once it enters the DOM.
        // We'll render to the DOM when the input gains focus and remove it when the input loses focus.
        // We'll use aria-hidden to prevent default aria information from being read by the screen
        // reader.

        removeOptionMessage = ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", {
          "aria-live": "assertive",
          id: removeOptionMessageId
        }, removeOptionMessageContent));
      }

      var placeholderMessage;

      if (placeholder && selectedOptions && !selectedOptions.length && !searchValue) {
        placeholderMessage = ___EmotionJSX("p", {
          className: "euiComboBoxPlaceholder"
        }, placeholder);
      }

      var clickProps = {};

      if (!isDisabled && onClear && hasSelectedOptions) {
        clickProps.clear = {
          'data-test-subj': 'comboBoxClearButton',
          onClick: onClear
        };
      }

      var icon;

      if (!noIcon) {
        icon = {
          'aria-label': isListOpen ? 'Close list of options' : 'Open list of options',
          'data-test-subj': 'comboBoxToggleListButton',
          disabled: isDisabled,
          onClick: isListOpen && !isDisabled ? onCloseListClick : onOpenListClick,
          ref: toggleButtonRef,
          side: 'right',
          type: 'arrowDown'
        };
      }

      var wrapClasses = classNames('euiComboBox__inputWrap', {
        'euiComboBox__inputWrap--compressed': compressed,
        'euiComboBox__inputWrap--fullWidth': fullWidth,
        'euiComboBox__inputWrap--noWrap': singleSelection,
        'euiComboBox__inputWrap-isLoading': isLoading,
        'euiComboBox__inputWrap-isClearable': onClear,
        'euiComboBox__inputWrap--inGroup': prepend || append
      });
      return ___EmotionJSX(EuiFormControlLayout, _extends({
        icon: icon
      }, clickProps, {
        inputId: id,
        isLoading: isLoading,
        compressed: compressed,
        fullWidth: fullWidth,
        prepend: prepend,
        append: append
      }), ___EmotionJSX("div", {
        className: wrapClasses,
        "data-test-subj": "comboBoxInput",
        onClick: onClick,
        tabIndex: -1 // becomes onBlur event's relatedTarget, otherwise relatedTarget is null when clicking on this div

      }, !singleSelection || !searchValue ? pills : null, placeholderMessage, ___EmotionJSX(AutosizeInput, {
        "aria-label": ariaLabel,
        "aria-labelledby": ariaLabelledby,
        "aria-activedescendant": focusedOptionId,
        "aria-controls": isListOpen ? rootId('listbox') : '',
        className: "euiComboBox__input",
        "data-test-subj": "comboBoxSearchInput",
        disabled: isDisabled,
        id: id,
        inputRef: inputRef,
        onBlur: this.onBlur,
        onChange: this.inputOnChange,
        onFocus: this.onFocus,
        ref: this.inputRefCallback,
        role: "textbox",
        style: {
          fontSize: 14
        },
        value: searchValue,
        autoFocus: autoFocus
      }), removeOptionMessage));
    }
  }]);

  return EuiComboBoxInput;
}(Component);
EuiComboBoxInput.propTypes = {
  autoSizeInputRef: PropTypes.any,
  compressed: PropTypes.bool.isRequired,
  focusedOptionId: PropTypes.string,
  fullWidth: PropTypes.bool,
  hasSelectedOptions: PropTypes.bool.isRequired,
  id: PropTypes.string,
  inputRef: PropTypes.any,
  isDisabled: PropTypes.bool,
  isListOpen: PropTypes.bool.isRequired,
  noIcon: PropTypes.bool.isRequired,
  onBlur: PropTypes.any,
  onChange: PropTypes.func,
  onClear: PropTypes.func,
  onClick: PropTypes.func,
  onCloseListClick: PropTypes.func.isRequired,
  onFocus: PropTypes.any.isRequired,
  onOpenListClick: PropTypes.func.isRequired,
  onRemoveOption: PropTypes.func,
  placeholder: PropTypes.string,
  rootId: PropTypes.any.isRequired,
  searchValue: PropTypes.string.isRequired,
  selectedOptions: PropTypes.arrayOf(PropTypes.shape({
    isGroupLabelOption: PropTypes.bool,
    label: PropTypes.string.isRequired,
    key: PropTypes.string,
    options: PropTypes.arrayOf(PropTypes.shape({
      isGroupLabelOption: PropTypes.bool,
      label: PropTypes.string.isRequired,
      key: PropTypes.string,
      options: PropTypes.arrayOf(PropTypes.any.isRequired),
      value: PropTypes.any,
      className: PropTypes.string,
      "aria-label": PropTypes.string,
      "data-test-subj": PropTypes.string
    }).isRequired),
    value: PropTypes.any,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired),
  singleSelection: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.shape({
    asPlainText: PropTypes.bool
  }).isRequired]),
  toggleButtonRef: PropTypes.any,
  updatePosition: PropTypes.func.isRequired,
  value: PropTypes.string,
  prepend: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),
  append: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),
  isLoading: PropTypes.bool,
  autoFocus: PropTypes.bool,
  "aria-label": PropTypes.string,
  "aria-labelledby": PropTypes.string,
  className: PropTypes.string,
  "data-test-subj": PropTypes.string
};