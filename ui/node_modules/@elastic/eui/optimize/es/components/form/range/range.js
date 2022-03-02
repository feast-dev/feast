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
import { isWithinRange } from '../../../services/number';
import { EuiInputPopover } from '../../popover';
import { htmlIdGenerator } from '../../../services/accessibility';
import { EuiRangeHighlight } from './range_highlight';
import { EuiRangeInput } from './range_input';
import { EuiRangeLabel } from './range_label';
import { EuiRangeSlider } from './range_slider';
import { EuiRangeTooltip } from './range_tooltip';
import { EuiRangeTrack } from './range_track';
import { EuiRangeWrapper } from './range_wrapper';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiRange = /*#__PURE__*/function (_Component) {
  _inherits(EuiRange, _Component);

  var _super = _createSuper(EuiRange);

  function EuiRange() {
    var _this;

    _classCallCheck(this, EuiRange);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "preventPopoverClose", false);

    _defineProperty(_assertThisInitialized(_this), "state", {
      id: _this.props.id || htmlIdGenerator()(),
      isPopoverOpen: false
    });

    _defineProperty(_assertThisInitialized(_this), "handleOnChange", function (e) {
      var isValid = isWithinRange(_this.props.min, _this.props.max, e.currentTarget.value);

      if (_this.props.onChange) {
        _this.props.onChange(e, isValid);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onInputFocus", function (e) {
      if (_this.props.onFocus) {
        _this.props.onFocus(e);
      }

      _this.setState({
        isPopoverOpen: true
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onInputBlur", function (e) {
      return setTimeout(function () {
        // Safari does not recognize any focus-related eventing for input[type=range]
        // making it impossible to capture its state using active/focus/relatedTarget
        // Instead, a prevention flag is set on mousedown, with a waiting period here.
        // Mousedown is viable because in the popover case, it is inaccessible via keyboard (intentionally)
        if (_this.preventPopoverClose) {
          _this.preventPopoverClose = false;
          return;
        }

        if (_this.props.onBlur) {
          _this.props.onBlur(e);
        }

        _this.closePopover();
      }, 200);
    });

    _defineProperty(_assertThisInitialized(_this), "closePopover", function () {
      _this.preventPopoverClose = false;

      _this.setState({
        isPopoverOpen: false
      });
    });

    return _this;
  }

  _createClass(EuiRange, [{
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props = this.props,
          className = _this$props.className,
          compressed = _this$props.compressed,
          disabled = _this$props.disabled,
          fullWidth = _this$props.fullWidth,
          isLoading = _this$props.isLoading,
          readOnly = _this$props.readOnly,
          propsId = _this$props.id,
          max = _this$props.max,
          min = _this$props.min,
          name = _this$props.name,
          step = _this$props.step,
          showLabels = _this$props.showLabels,
          showInput = _this$props.showInput,
          showTicks = _this$props.showTicks,
          tickInterval = _this$props.tickInterval,
          ticks = _this$props.ticks,
          levels = _this$props.levels,
          showRange = _this$props.showRange,
          showValue = _this$props.showValue,
          valueAppend = _this$props.valueAppend,
          valuePrepend = _this$props.valuePrepend,
          onBlur = _this$props.onBlur,
          onChange = _this$props.onChange,
          onFocus = _this$props.onFocus,
          value = _this$props.value,
          style = _this$props.style,
          tabIndex = _this$props.tabIndex,
          isInvalid = _this$props.isInvalid,
          rest = _objectWithoutProperties(_this$props, ["className", "compressed", "disabled", "fullWidth", "isLoading", "readOnly", "id", "max", "min", "name", "step", "showLabels", "showInput", "showTicks", "tickInterval", "ticks", "levels", "showRange", "showValue", "valueAppend", "valuePrepend", "onBlur", "onChange", "onFocus", "value", "style", "tabIndex", "isInvalid"]);

      var id = this.state.id;
      var digitTolerance = Math.max(String(min).length, String(max).length);
      var showInputOnly = showInput === 'inputWithPopover';
      var canShowDropdown = showInputOnly && !readOnly && !disabled;
      var theInput = !!showInput ? ___EmotionJSX(EuiRangeInput, _extends({
        id: id,
        min: min,
        max: max,
        digitTolerance: digitTolerance,
        step: step,
        value: value,
        readOnly: readOnly,
        disabled: disabled,
        compressed: compressed,
        onChange: this.handleOnChange,
        name: name,
        onFocus: canShowDropdown ? this.onInputFocus : onFocus,
        onBlur: canShowDropdown ? this.onInputBlur : onBlur,
        fullWidth: showInputOnly && fullWidth,
        isLoading: showInputOnly && isLoading,
        isInvalid: isInvalid,
        autoSize: !showInputOnly
      }, rest)) : null;
      var classes = classNames('euiRange', {
        'euiRange--hasInput': showInput
      }, className);

      var theRange = ___EmotionJSX(EuiRangeWrapper, {
        className: classes,
        fullWidth: fullWidth,
        compressed: compressed
      }, showLabels && ___EmotionJSX(EuiRangeLabel, {
        side: "min",
        disabled: disabled
      }, min), ___EmotionJSX(EuiRangeTrack, {
        disabled: disabled,
        compressed: compressed,
        max: max,
        min: min,
        step: step,
        showTicks: showTicks,
        tickInterval: tickInterval,
        ticks: ticks,
        levels: levels,
        onChange: this.handleOnChange,
        value: value,
        "aria-hidden": showInput === true
      }, ___EmotionJSX(EuiRangeSlider, _extends({
        id: showInput ? undefined : id // Attach id only to the input if there is one
        ,
        name: name,
        min: min,
        max: max,
        step: step,
        value: value,
        disabled: disabled,
        compressed: compressed,
        onChange: this.handleOnChange,
        style: style,
        showTicks: showTicks,
        showRange: showRange,
        tabIndex: showInput ? -1 : tabIndex,
        onMouseDown: showInputOnly ? function () {
          return _this2.preventPopoverClose = true;
        } : undefined,
        onFocus: showInput === true ? undefined : onFocus,
        onBlur: showInputOnly ? this.onInputBlur : onBlur,
        "aria-hidden": showInput === true ? true : false
      }, rest)), showRange && this.isValid && ___EmotionJSX(EuiRangeHighlight, {
        compressed: compressed,
        showTicks: showTicks,
        min: Number(min),
        max: Number(max),
        lowerValue: Number(min),
        upperValue: Number(value)
      }), showValue && !!String(value).length && ___EmotionJSX(EuiRangeTooltip, {
        compressed: compressed,
        value: value,
        max: max,
        min: min,
        name: name,
        showTicks: showTicks,
        valuePrepend: valuePrepend,
        valueAppend: valueAppend
      })), showLabels && ___EmotionJSX(EuiRangeLabel, {
        side: "max",
        disabled: disabled
      }, max), showInput && !showInputOnly && ___EmotionJSX(React.Fragment, null, ___EmotionJSX("div", {
        className: showTicks || ticks ? 'euiRange__slimHorizontalSpacer' : 'euiRange__horizontalSpacer'
      }), theInput));

      var thePopover = showInputOnly ? ___EmotionJSX(EuiInputPopover, {
        className: "euiRange__popover",
        input: theInput // `showInputOnly` confirms existence
        ,
        fullWidth: fullWidth,
        isOpen: this.state.isPopoverOpen,
        closePopover: this.closePopover,
        disableFocusTrap: true
      }, theRange) : undefined;
      return thePopover ? thePopover : theRange;
    }
  }, {
    key: "isValid",
    get: function get() {
      return isWithinRange(this.props.min, this.props.max, this.props.value || '');
    }
  }]);

  return EuiRange;
}(Component);

_defineProperty(EuiRange, "defaultProps", {
  min: 0,
  max: 100,
  step: 1,
  fullWidth: false,
  compressed: false,
  isLoading: false,
  showLabels: false,
  showInput: false,
  showRange: false,
  showTicks: false,
  showValue: false,
  levels: []
});