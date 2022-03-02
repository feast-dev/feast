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