import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _typeof from "@babel/runtime/helpers/typeof";
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