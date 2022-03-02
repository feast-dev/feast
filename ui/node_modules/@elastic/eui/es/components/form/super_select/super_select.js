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
import { EuiScreenReaderOnly } from '../../accessibility';
import { htmlIdGenerator, keys } from '../../../services';
import { EuiSuperSelectControl } from './super_select_control';
import { EuiInputPopover } from '../../popover';
import { EuiContextMenuItem } from '../../context_menu';
import { EuiI18n } from '../../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
var ShiftDirection;

(function (ShiftDirection) {
  ShiftDirection["BACK"] = "back";
  ShiftDirection["FORWARD"] = "forward";
})(ShiftDirection || (ShiftDirection = {}));

export var EuiSuperSelect = /*#__PURE__*/function (_Component) {
  _inherits(EuiSuperSelect, _Component);

  var _super = _createSuper(EuiSuperSelect);

  function EuiSuperSelect() {
    var _this;

    _classCallCheck(this, EuiSuperSelect);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "itemNodes", []);

    _defineProperty(_assertThisInitialized(_this), "_isMounted", false);

    _defineProperty(_assertThisInitialized(_this), "describedById", htmlIdGenerator('euiSuperSelect_')('_screenreaderDescribeId'));

    _defineProperty(_assertThisInitialized(_this), "labelledById", htmlIdGenerator('euiSuperSelect_')('_screenreaderLabelId'));

    _defineProperty(_assertThisInitialized(_this), "state", {
      isPopoverOpen: _this.props.isOpen || false
    });

    _defineProperty(_assertThisInitialized(_this), "setItemNode", function (node, index) {
      _this.itemNodes[index] = node;
    });

    _defineProperty(_assertThisInitialized(_this), "openPopover", function () {
      _this.setState({
        isPopoverOpen: true
      });

      var focusSelected = function focusSelected() {
        var indexOfSelected = _this.props.options.reduce(function (indexOfSelected, option, index) {
          if (indexOfSelected != null) return indexOfSelected;
          if (option == null) return null;
          return option.value === _this.props.valueOfSelected ? index : null;
        }, null);

        requestAnimationFrame(function () {
          if (!_this._isMounted) {
            return;
          }

          if (_this.props.valueOfSelected != null) {
            if (indexOfSelected != null) {
              _this.focusItemAt(indexOfSelected);
            } else {
              focusSelected();
            }
          } else {
            var firstFocusableOption = _this.props.options.findIndex(function (_ref) {
              var disabled = _ref.disabled;
              return disabled !== true;
            });

            _this.focusItemAt(firstFocusableOption);
          }

          if (_this.props.onFocus) {
            _this.props.onFocus();
          }
        });
      };

      requestAnimationFrame(focusSelected);
    });

    _defineProperty(_assertThisInitialized(_this), "closePopover", function () {
      _this.setState({
        isPopoverOpen: false
      });

      if (_this.props.onBlur) {
        _this.props.onBlur();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "itemClicked", function (value) {
      _this.closePopover();

      if (_this.props.onChange) {
        _this.props.onChange(value);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onSelectKeyDown", function (event) {
      if (event.key === keys.ARROW_UP || event.key === keys.ARROW_DOWN) {
        event.preventDefault();
        event.stopPropagation();

        _this.openPopover();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onItemKeyDown", function (event) {
      switch (event.key) {
        case keys.ESCAPE:
          // close the popover and prevent ancestors from handling
          event.preventDefault();
          event.stopPropagation();

          _this.closePopover();

          break;

        case keys.TAB:
          // no-op
          event.preventDefault();
          event.stopPropagation();
          break;

        case keys.ARROW_UP:
          event.preventDefault();
          event.stopPropagation();

          _this.shiftFocus(ShiftDirection.BACK);

          break;

        case keys.ARROW_DOWN:
          event.preventDefault();
          event.stopPropagation();

          _this.shiftFocus(ShiftDirection.FORWARD);

          break;
      }
    });

    return _this;
  }

  _createClass(EuiSuperSelect, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      this._isMounted = true;

      if (this.props.isOpen) {
        this.openPopover();
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this._isMounted = false;
    }
  }, {
    key: "focusItemAt",
    value: function focusItemAt(index) {
      var targetElement = this.itemNodes[index];

      if (targetElement != null) {
        targetElement.focus();
      }
    }
  }, {
    key: "shiftFocus",
    value: function shiftFocus(direction) {
      var currentIndex = this.itemNodes.indexOf(document.activeElement);
      var targetElementIndex;

      if (currentIndex === -1) {
        // somehow the select options has lost focus
        targetElementIndex = 0;
      } else {
        if (direction === ShiftDirection.BACK) {
          targetElementIndex = currentIndex === 0 ? this.itemNodes.length - 1 : currentIndex - 1;
        } else {
          targetElementIndex = currentIndex === this.itemNodes.length - 1 ? 0 : currentIndex + 1;
        }
      }

      this.focusItemAt(targetElementIndex);
    }
  }, {
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props = this.props,
          className = _this$props.className,
          options = _this$props.options,
          valueOfSelected = _this$props.valueOfSelected,
          onChange = _this$props.onChange,
          isOpen = _this$props.isOpen,
          isInvalid = _this$props.isInvalid,
          hasDividers = _this$props.hasDividers,
          itemClassName = _this$props.itemClassName,
          itemLayoutAlign = _this$props.itemLayoutAlign,
          fullWidth = _this$props.fullWidth,
          popoverProps = _this$props.popoverProps,
          compressed = _this$props.compressed,
          rest = _objectWithoutProperties(_this$props, ["className", "options", "valueOfSelected", "onChange", "isOpen", "isInvalid", "hasDividers", "itemClassName", "itemLayoutAlign", "fullWidth", "popoverProps", "compressed"]);

      var popoverClasses = classNames('euiSuperSelect', popoverProps === null || popoverProps === void 0 ? void 0 : popoverProps.className);
      var buttonClasses = classNames({
        'euiSuperSelect--isOpen__button': this.state.isPopoverOpen
      }, className);
      var itemClasses = classNames('euiSuperSelect__item', {
        'euiSuperSelect__item--hasDividers': hasDividers
      }, itemClassName);

      var button = ___EmotionJSX(EuiSuperSelectControl, _extends({
        screenReaderId: this.labelledById,
        options: options,
        value: valueOfSelected,
        onClick: this.state.isPopoverOpen ? this.closePopover : this.openPopover,
        onKeyDown: this.onSelectKeyDown,
        className: buttonClasses,
        fullWidth: fullWidth,
        isInvalid: isInvalid,
        compressed: compressed
      }, rest));

      var items = options.map(function (option, index) {
        var value = option.value,
            dropdownDisplay = option.dropdownDisplay,
            inputDisplay = option.inputDisplay,
            optionRest = _objectWithoutProperties(option, ["value", "dropdownDisplay", "inputDisplay"]);

        return ___EmotionJSX(EuiContextMenuItem, _extends({
          key: index,
          className: itemClasses,
          icon: valueOfSelected === value ? 'check' : 'empty',
          onClick: function onClick() {
            return _this2.itemClicked(value);
          },
          onKeyDown: _this2.onItemKeyDown,
          layoutAlign: itemLayoutAlign,
          buttonRef: function buttonRef(node) {
            return _this2.setItemNode(node, index);
          },
          role: "option",
          id: value,
          "aria-selected": valueOfSelected === value
        }, optionRest), dropdownDisplay || inputDisplay);
      });
      return ___EmotionJSX(EuiInputPopover, _extends({
        closePopover: this.closePopover,
        panelPaddingSize: "none"
      }, popoverProps, {
        className: popoverClasses,
        isOpen: isOpen || this.state.isPopoverOpen,
        input: button,
        fullWidth: fullWidth
      }), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", {
        id: this.describedById
      }, ___EmotionJSX(EuiI18n, {
        token: "euiSuperSelect.screenReaderAnnouncement",
        default: "You are in a form selector and must select a single option. Use the up and down keys to navigate or escape to close."
      }))), ___EmotionJSX("div", {
        "aria-labelledby": this.labelledById,
        "aria-describedby": this.describedById,
        className: "euiSuperSelect__listbox",
        role: "listbox",
        "aria-activedescendant": valueOfSelected,
        tabIndex: 0
      }, items));
    }
  }]);

  return EuiSuperSelect;
}(Component);

_defineProperty(EuiSuperSelect, "defaultProps", {
  hasDividers: false,
  fullWidth: false,
  compressed: false,
  isInvalid: false,
  isLoading: false
});

EuiSuperSelect.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  compressed: PropTypes.bool,
  fullWidth: PropTypes.bool,
  isInvalid: PropTypes.bool,
  isLoading: PropTypes.bool,
  readOnly: PropTypes.bool,
  name: PropTypes.string,

  /**
     * Creates an input group with element(s) coming before input.
     * `string` | `ReactElement` or an array of these
     */
  prepend: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),

  /**
     * Creates an input group with element(s) coming after input.
     * `string` | `ReactElement` or an array of these
     */
  append: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),

  /**
     * Creates a semantic label ID for the `div[role="listbox"]` that's opened on click or keypress.
     * __Generated and passed down by `EuiSuperSelect`.__
     */
  screenReaderId: PropTypes.string,

  /**
       * Pass an array of options that must at least include:
       * `value`: storing unique value of item,
       * `inputDisplay`: what shows inside the form input when selected
       * `dropdownDisplay` (optional): what shows for the item in the dropdown
       */
  options: PropTypes.arrayOf(PropTypes.shape({
    value: PropTypes.any.isRequired,
    inputDisplay: PropTypes.node,
    dropdownDisplay: PropTypes.node,
    disabled: PropTypes.bool,
    "data-test-subj": PropTypes.string
  }).isRequired).isRequired,
  valueOfSelected: PropTypes.any,

  /**
       * Classes for the context menu item
       */
  itemClassName: PropTypes.string,

  /**
       * You must pass an `onChange` function to handle the update of the value
       */
  onChange: PropTypes.func,
  onFocus: PropTypes.func,
  onBlur: PropTypes.func,

  /**
       * Change to `true` if you want horizontal lines between options.
       * This is best used when options are multi-line.
       */
  hasDividers: PropTypes.bool,

  /**
       * Change `EuiContextMenuItem` layout position of icon
       */
  itemLayoutAlign: PropTypes.oneOf(["center", "top", "bottom"]),

  /**
       * Controls whether the options are shown. Default: false
       */
  isOpen: PropTypes.bool,

  /**
       * Optional props to pass to the underlying [EuiPopover](/#/layout/popover).
       * Allows fine-grained control of the popover dropdown menu, including
       * `repositionOnScroll` for EuiSuperSelects used within scrollable containers,
       * and customizing popover panel styling.
       *
       * Does not accept a nested `popoverProps.isOpen` property - use the top level
       * `isOpen` API instead.
       */
  popoverProps: PropTypes.any
};