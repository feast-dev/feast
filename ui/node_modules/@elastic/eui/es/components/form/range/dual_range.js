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
import { keys } from '../../../services';
import { isWithinRange } from '../../../services/number';
import { EuiInputPopover } from '../../popover';
import { EuiFormControlLayoutDelimited } from '../form_control_layout';
import { htmlIdGenerator } from '../../../services/accessibility';
import { EuiRangeDraggable } from './range_draggable';
import { EuiRangeHighlight } from './range_highlight';
import { EuiRangeInput } from './range_input';
import { EuiRangeLabel } from './range_label';
import { EuiRangeSlider } from './range_slider';
import { EuiRangeThumb } from './range_thumb';
import { EuiRangeTrack } from './range_track';
import { EuiRangeWrapper } from './range_wrapper';
import { calculateThumbPosition } from './utils';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiDualRange = /*#__PURE__*/function (_Component) {
  _inherits(EuiDualRange, _Component);

  var _super = _createSuper(EuiDualRange);

  function EuiDualRange() {
    var _this;

    _classCallCheck(this, EuiDualRange);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "state", {
      id: _this.props.id || htmlIdGenerator()(),
      hasFocus: false,
      rangeSliderRefAvailable: false,
      isPopoverOpen: false,
      rangeWidth: undefined,
      isVisible: true // used to trigger a rerender if initial element width is 0

    });

    _defineProperty(_assertThisInitialized(_this), "preventPopoverClose", false);

    _defineProperty(_assertThisInitialized(_this), "rangeSliderRef", null);

    _defineProperty(_assertThisInitialized(_this), "handleRangeSliderRefUpdate", function (ref) {
      _this.rangeSliderRef = ref;

      _this.setState({
        rangeSliderRefAvailable: !!ref,
        rangeWidth: !!ref ? ref.clientWidth : null
      });
    });

    _defineProperty(_assertThisInitialized(_this), "leftPosition", 0);

    _defineProperty(_assertThisInitialized(_this), "dragAcc", 0);

    _defineProperty(_assertThisInitialized(_this), "_determineInvalidThumbMovement", function (newVal, lower, upper, e) {
      // If the values are invalid, find whether the new value is in the upper
      // or lower half and move the appropriate handle to the new value,
      // while the other handle gets moved to the opposite bound (if invalid)
      var lowerHalf = Math.abs(_this.props.max - _this.props.min) / 2 + _this.props.min;

      var newValIsLow = isWithinRange(_this.props.min, lowerHalf, newVal);

      if (newValIsLow) {
        lower = newVal;
        upper = !_this.upperValueIsValid ? _this.props.max : upper;
      } else {
        lower = !_this.lowerValueIsValid ? _this.props.min : lower;
        upper = newVal;
      }

      _this._handleOnChange(lower, upper, e);
    });

    _defineProperty(_assertThisInitialized(_this), "_determineValidThumbMovement", function (newVal, lower, upper, e) {
      // Lower thumb targeted or right-moving swap has occurred
      if (Math.abs(lower - newVal) < Math.abs(upper - newVal)) {
        lower = newVal;
      } // Upper thumb targeted or left-moving swap has occurred
      else {
          upper = newVal;
        }

      _this._handleOnChange(lower, upper, e);
    });

    _defineProperty(_assertThisInitialized(_this), "_determineThumbMovement", function (newVal, e) {
      // Determine thumb movement based on slider interaction
      if (!_this.isValid) {
        // Non-standard positioning follows
        _this._determineInvalidThumbMovement(newVal, _this.lowerValue, _this.upperValue, e);
      } else {
        // Standard positioning based on click event proximity to thumb locations
        _this._determineValidThumbMovement(newVal, _this.lowerValue, _this.upperValue, e);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "_handleOnChange", function (lower, upper, e) {
      var isValid = isWithinRange(_this.props.min, upper, lower) && isWithinRange(lower, _this.props.max, upper);

      _this.props.onChange([lower, upper], isValid, e);
    });

    _defineProperty(_assertThisInitialized(_this), "handleSliderChange", function (e) {
      _this._determineThumbMovement(Number(e.currentTarget.value), e);
    });

    _defineProperty(_assertThisInitialized(_this), "_resetToRangeEnds", function (e) {
      // Arbitrary decision to pass `min` instead of `max`. Result is the same.
      _this._determineInvalidThumbMovement(_this.props.min, _this.lowerValue, _this.upperValue, e);
    });

    _defineProperty(_assertThisInitialized(_this), "_isDirectionalKeyPress", function (event) {
      return [keys.ARROW_UP, keys.ARROW_RIGHT, keys.ARROW_DOWN, keys.ARROW_LEFT].indexOf(event.key) > -1;
    });

    _defineProperty(_assertThisInitialized(_this), "handleInputKeyDown", function (e) {
      // Relevant only when initial values are both `''` and `showInput` is set
      if (_this._isDirectionalKeyPress(e) && !_this.isValid) {
        e.preventDefault();

        _this._resetToRangeEnds(e);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "handleLowerInputChange", function (e) {
      _this._handleOnChange(e.target.value, _this.upperValue, e);
    });

    _defineProperty(_assertThisInitialized(_this), "handleUpperInputChange", function (e) {
      _this._handleOnChange(_this.lowerValue, e.target.value, e);
    });

    _defineProperty(_assertThisInitialized(_this), "_handleKeyDown", function (value, event) {
      var newVal = Number(value);
      var stepRemainder = 0;
      var step = _this.props.step || 1;

      switch (event.key) {
        case keys.ARROW_UP:
        case keys.ARROW_RIGHT:
          event.preventDefault();
          newVal += step;
          stepRemainder = (newVal - _this.props.min) % step;

          if (step !== 1 && stepRemainder > 0) {
            newVal = newVal - stepRemainder;
          }

          break;

        case keys.ARROW_DOWN:
        case keys.ARROW_LEFT:
          event.preventDefault();
          newVal -= step;
          stepRemainder = (newVal - _this.props.min) % step;

          if (step !== 1 && stepRemainder > 0) {
            newVal = newVal + (step - stepRemainder);
          }

          break;
      }

      return newVal;
    });

    _defineProperty(_assertThisInitialized(_this), "handleLowerKeyDown", function (event) {
      var lower = _this.lowerValue;

      switch (event.key) {
        case keys.TAB:
          return;

        default:
          if (!_this.lowerValueIsValid) {
            // Relevant only when initial value is `''` and `showInput` is not set
            event.preventDefault();

            _this._resetToRangeEnds(event);

            return;
          }

          lower = _this._handleKeyDown(lower, event);
      }

      if (lower >= _this.upperValue || lower < _this.props.min) return;

      _this._handleOnChange(lower, _this.upperValue, event);
    });

    _defineProperty(_assertThisInitialized(_this), "handleUpperKeyDown", function (event) {
      var upper = _this.upperValue;

      switch (event.key) {
        case keys.TAB:
          return;

        default:
          if (!_this.upperValueIsValid) {
            // Relevant only when initial value is `''` and `showInput` is not set
            event.preventDefault();

            _this._resetToRangeEnds(event);

            return;
          }

          upper = _this._handleKeyDown(upper, event);
      }

      if (upper <= _this.lowerValue || upper > _this.props.max) return;

      _this._handleOnChange(_this.lowerValue, upper, event);
    });

    _defineProperty(_assertThisInitialized(_this), "handleDraggableKeyDown", function (event) {
      var lower = _this.lowerValue;
      var upper = _this.upperValue;

      switch (event.key) {
        case keys.TAB:
          return;

        default:
          lower = _this._handleKeyDown(lower, event);
          upper = _this._handleKeyDown(upper, event);
      }

      if (lower >= _this.upperValue || lower < _this.props.min) return;
      if (upper <= _this.lowerValue || upper > _this.props.max) return;

      _this._handleOnChange(lower, upper, event);
    });

    _defineProperty(_assertThisInitialized(_this), "calculateThumbPositionStyle", function (value, width) {
      var trackWidth = _this.props.showInput === 'inputWithPopover' && !!width ? width : _this.rangeSliderRef.clientWidth;
      var position = calculateThumbPosition(value, _this.props.min, _this.props.max, trackWidth);
      return {
        left: "".concat(position, "%")
      };
    });

    _defineProperty(_assertThisInitialized(_this), "toggleHasFocus", function () {
      var shouldFocused = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : !_this.state.hasFocus;

      _this.setState({
        hasFocus: shouldFocused
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onThumbFocus", function (e) {
      if (_this.props.onFocus) {
        _this.props.onFocus(e);
      }

      _this.toggleHasFocus(true);
    });

    _defineProperty(_assertThisInitialized(_this), "onThumbBlur", function (e) {
      if (_this.props.onBlur) {
        _this.props.onBlur(e);
      }

      _this.toggleHasFocus(false);
    });

    _defineProperty(_assertThisInitialized(_this), "onInputFocus", function (e) {
      if (_this.props.onFocus) {
        _this.props.onFocus(e);
      }

      _this.preventPopoverClose = true;

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

    _defineProperty(_assertThisInitialized(_this), "onResize", function (width) {
      _this.setState({
        rangeWidth: width
      });
    });

    _defineProperty(_assertThisInitialized(_this), "getNearestStep", function (value) {
      var steps = (_this.props.max - _this.props.min) / _this.props.step;
      var approx = Math.round((value - _this.props.min) * steps / (_this.props.max - _this.props.min)) / steps;
      var bound = Math.min(Math.max(approx, 0), 1);
      var nearest = bound * (_this.props.max - _this.props.min) + _this.props.min;
      return Number(nearest.toPrecision(10)) * 100 / 100;
    });

    _defineProperty(_assertThisInitialized(_this), "handleDrag", function (x, isFirstInteraction) {
      if (isFirstInteraction) {
        _this.leftPosition = x;
        _this.dragAcc = 0;
      }

      var _this$props = _this.props,
          min = _this$props.min,
          max = _this$props.max;
      var lowerValue = Number(_this.lowerValue);
      var upperValue = Number(_this.upperValue);
      var delta = _this.leftPosition - x;
      _this.leftPosition = x;
      _this.dragAcc = _this.dragAcc + delta;
      var percentageOfArea = _this.dragAcc / _this.rangeSliderRef.clientWidth;
      var percentageOfRange = percentageOfArea * (max - min);

      var newLower = _this.getNearestStep(lowerValue - percentageOfRange);

      var newUpper = _this.getNearestStep(upperValue - percentageOfRange);

      var noMovement = newLower === lowerValue;
      var isMin = min === lowerValue && min === newLower;
      var isMax = max === upperValue && max === newUpper;
      var isOutOfRange = newLower < min || newUpper > max;
      if (noMovement || isMin || isMax || isOutOfRange) return;

      _this._handleOnChange(newLower, newUpper);

      _this.dragAcc = 0;
    });

    return _this;
  }

  _createClass(EuiDualRange, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      if (this.rangeSliderRef && this.rangeSliderRef.clientWidth === 0) {
        // Safe to call `setState` inside conditional
        // https://reactjs.org/docs/react-component.html#componentdidmount
        // eslint-disable-next-line react/no-did-mount-set-state
        this.setState({
          isVisible: false
        });
      }
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate() {
      var _this$rangeSliderRef;

      if (((_this$rangeSliderRef = this.rangeSliderRef) === null || _this$rangeSliderRef === void 0 ? void 0 : _this$rangeSliderRef.clientWidth) && !this.state.isVisible) {
        // Safe to call `setState` inside conditional
        // https://reactjs.org/docs/react-component.html#componentdidupdate
        // eslint-disable-next-line  react/no-did-update-set-state
        this.setState({
          isVisible: true
        });
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props2 = this.props,
          className = _this$props2.className,
          compressed = _this$props2.compressed,
          disabled = _this$props2.disabled,
          fullWidth = _this$props2.fullWidth,
          readOnly = _this$props2.readOnly,
          propsId = _this$props2.id,
          max = _this$props2.max,
          min = _this$props2.min,
          name = _this$props2.name,
          step = _this$props2.step,
          showLabels = _this$props2.showLabels,
          showInput = _this$props2.showInput,
          showTicks = _this$props2.showTicks,
          tickInterval = _this$props2.tickInterval,
          ticks = _this$props2.ticks,
          levels = _this$props2.levels,
          onBlur = _this$props2.onBlur,
          onChange = _this$props2.onChange,
          onFocus = _this$props2.onFocus,
          showRange = _this$props2.showRange,
          value = _this$props2.value,
          style = _this$props2.style,
          isInvalid = _this$props2.isInvalid,
          append = _this$props2.append,
          prepend = _this$props2.prepend,
          minInputProps = _this$props2.minInputProps,
          maxInputProps = _this$props2.maxInputProps,
          isDraggable = _this$props2.isDraggable,
          rest = _objectWithoutProperties(_this$props2, ["className", "compressed", "disabled", "fullWidth", "readOnly", "id", "max", "min", "name", "step", "showLabels", "showInput", "showTicks", "tickInterval", "ticks", "levels", "onBlur", "onChange", "onFocus", "showRange", "value", "style", "isInvalid", "append", "prepend", "minInputProps", "maxInputProps", "isDraggable"]);

      var id = this.state.id;
      var digitTolerance = Math.max(String(min).length, String(max).length);
      var showInputOnly = showInput === 'inputWithPopover';
      var canShowDropdown = showInputOnly && !readOnly && !disabled;
      var minInput = !!showInput ? ___EmotionJSX(EuiRangeInput // Overridable props
      , _extends({
        "aria-describedby": this.props['aria-describedby'],
        "aria-label": this.props['aria-label']
      }, minInputProps, {
        // Non-overridable props
        digitTolerance: digitTolerance,
        side: "min",
        min: min,
        max: Number(this.upperValue),
        step: step,
        value: this.lowerValue,
        disabled: disabled,
        compressed: compressed,
        onChange: this.handleLowerInputChange,
        onKeyDown: this.handleInputKeyDown,
        name: "".concat(name, "-minValue"),
        onFocus: canShowDropdown ? this.onInputFocus : onFocus,
        onBlur: canShowDropdown ? this.onInputBlur : onBlur,
        readOnly: readOnly,
        autoSize: !showInputOnly,
        fullWidth: !!showInputOnly && fullWidth,
        isInvalid: isInvalid,
        controlOnly: showInputOnly,
        onMouseDown: showInputOnly ? function () {
          return _this2.preventPopoverClose = true;
        } : undefined
      })) : undefined;
      var maxInput = !!showInput ? ___EmotionJSX(EuiRangeInput // Overridable props
      , _extends({
        "aria-describedby": this.props['aria-describedby'],
        "aria-label": this.props['aria-label']
      }, maxInputProps, {
        // Non-overridable props
        digitTolerance: digitTolerance,
        side: "max",
        min: Number(this.lowerValue),
        max: max,
        step: step,
        value: this.upperValue,
        disabled: disabled,
        compressed: compressed,
        onChange: this.handleUpperInputChange,
        onKeyDown: this.handleInputKeyDown,
        name: "".concat(name, "-maxValue"),
        onFocus: canShowDropdown ? this.onInputFocus : onFocus,
        onBlur: canShowDropdown ? this.onInputBlur : onBlur,
        readOnly: readOnly,
        autoSize: !showInputOnly,
        fullWidth: !!showInputOnly && fullWidth,
        controlOnly: showInputOnly,
        isInvalid: isInvalid,
        onMouseDown: showInputOnly ? function () {
          return _this2.preventPopoverClose = true;
        } : undefined
      })) : undefined;
      var classes = classNames('euiDualRange', className);
      var leftThumbPosition = this.state.rangeSliderRefAvailable ? this.calculateThumbPositionStyle(Number(this.lowerValue) || min, this.state.rangeWidth) : {
        left: '0'
      };
      var rightThumbPosition = this.state.rangeSliderRefAvailable ? this.calculateThumbPositionStyle(Number(this.upperValue) || max, this.state.rangeWidth) : {
        left: '0'
      };

      var theRange = ___EmotionJSX(EuiRangeWrapper, {
        className: classes,
        fullWidth: fullWidth,
        compressed: compressed
      }, showInput && !showInputOnly && ___EmotionJSX(React.Fragment, null, minInput, ___EmotionJSX("div", {
        className: showTicks || ticks ? 'euiRange__slimHorizontalSpacer' : 'euiRange__horizontalSpacer'
      })), showLabels && ___EmotionJSX(EuiRangeLabel, {
        side: "min",
        disabled: disabled
      }, min), ___EmotionJSX(EuiRangeTrack, {
        compressed: compressed,
        disabled: disabled,
        max: max,
        min: min,
        step: step,
        showTicks: showTicks,
        tickInterval: tickInterval,
        ticks: ticks,
        levels: levels,
        onChange: this.handleSliderChange,
        value: value,
        "aria-hidden": showInput === true
      }, ___EmotionJSX(EuiRangeSlider, _extends({
        className: "euiDualRange__slider",
        ref: this.handleRangeSliderRefUpdate,
        id: id,
        name: name,
        min: min,
        max: max,
        step: step,
        disabled: disabled,
        compressed: compressed,
        onChange: this.handleSliderChange,
        style: style,
        showTicks: showTicks,
        hasFocus: this.state.hasFocus,
        "aria-hidden": true,
        tabIndex: -1,
        showRange: showRange,
        onFocus: onFocus,
        onBlur: onBlur
      }, rest)), showRange && this.isValid && ___EmotionJSX(EuiRangeHighlight, {
        compressed: compressed,
        hasFocus: this.state.hasFocus,
        showTicks: showTicks,
        min: Number(min),
        max: Number(max),
        lowerValue: Number(this.lowerValue),
        upperValue: Number(this.upperValue)
      }), this.state.rangeSliderRefAvailable && ___EmotionJSX(React.Fragment, null, isDraggable && this.isValid && ___EmotionJSX(EuiRangeDraggable, {
        min: min,
        max: max,
        value: [Number(this.lowerValue), Number(this.upperValue)],
        disabled: disabled,
        lowerPosition: leftThumbPosition.left,
        upperPosition: rightThumbPosition.left,
        showTicks: showTicks,
        compressed: compressed,
        onChange: this.handleDrag,
        onFocus: this.onThumbFocus,
        onBlur: this.onThumbBlur,
        onKeyDown: this.handleDraggableKeyDown,
        "aria-describedby": this.props['aria-describedby'],
        "aria-label": this.props['aria-label']
      }), ___EmotionJSX(EuiRangeThumb, {
        min: min,
        max: Number(this.upperValue),
        value: this.lowerValue,
        disabled: disabled,
        showTicks: showTicks,
        showInput: !!showInput,
        onKeyDown: this.handleLowerKeyDown,
        onFocus: this.onThumbFocus,
        onBlur: this.onThumbBlur,
        style: leftThumbPosition,
        "aria-describedby": this.props['aria-describedby'],
        "aria-label": this.props['aria-label']
      }), ___EmotionJSX(EuiRangeThumb, {
        min: Number(this.lowerValue),
        max: max,
        value: this.upperValue,
        disabled: disabled,
        showTicks: showTicks,
        showInput: !!showInput,
        onKeyDown: this.handleUpperKeyDown,
        onFocus: this.onThumbFocus,
        onBlur: this.onThumbBlur,
        style: rightThumbPosition,
        "aria-describedby": this.props['aria-describedby'],
        "aria-label": this.props['aria-label']
      }))), showLabels && ___EmotionJSX(EuiRangeLabel, {
        disabled: disabled
      }, max), showInput && !showInputOnly && ___EmotionJSX(React.Fragment, null, ___EmotionJSX("div", {
        className: showTicks || ticks ? 'euiRange__slimHorizontalSpacer' : 'euiRange__horizontalSpacer'
      }), maxInput));

      var thePopover = showInputOnly ? ___EmotionJSX(EuiInputPopover, {
        className: "euiRange__popover",
        input: ___EmotionJSX(EuiFormControlLayoutDelimited, {
          startControl: minInput,
          endControl: maxInput,
          isDisabled: disabled,
          fullWidth: fullWidth,
          compressed: compressed,
          readOnly: readOnly,
          append: append,
          prepend: prepend
        }),
        fullWidth: fullWidth,
        isOpen: this.state.isPopoverOpen,
        closePopover: this.closePopover,
        disableFocusTrap: true,
        onPanelResize: this.onResize
      }, theRange) : undefined;
      return thePopover || theRange;
    }
  }, {
    key: "lowerValue",
    get: function get() {
      return this.props.value ? this.props.value[0] : this.props.min;
    }
  }, {
    key: "upperValue",
    get: function get() {
      return this.props.value ? this.props.value[1] : this.props.max;
    }
  }, {
    key: "lowerValueIsValid",
    get: function get() {
      return isWithinRange(this.props.min, this.upperValue, this.lowerValue);
    }
  }, {
    key: "upperValueIsValid",
    get: function get() {
      return isWithinRange(this.lowerValue, this.props.max, this.upperValue);
    }
  }, {
    key: "isValid",
    get: function get() {
      return this.lowerValueIsValid && this.upperValueIsValid;
    }
  }]);

  return EuiDualRange;
}(Component);

_defineProperty(EuiDualRange, "defaultProps", {
  min: 0,
  max: 100,
  step: 1,
  fullWidth: false,
  compressed: false,
  showLabels: false,
  showInput: false,
  showRange: true,
  showTicks: false,
  levels: []
});

EuiDualRange.propTypes = {
  value: PropTypes.any.isRequired,
  onBlur: PropTypes.func,
  onFocus: PropTypes.func,
  onChange: PropTypes.func.isRequired,
  fullWidth: PropTypes.bool,
  isInvalid: PropTypes.bool,

  /**
     * Create colored indicators for certain intervals
     */
  levels: PropTypes.arrayOf(PropTypes.shape({
    min: PropTypes.number.isRequired,
    max: PropTypes.number.isRequired,

    /**
       * Accepts one of `["primary", "success", "warning", "danger"]` or a valid CSS color value.
       */
    color: PropTypes.oneOfType([PropTypes.oneOf(["primary", "success", "warning", "danger"]).isRequired, PropTypes.any.isRequired]).isRequired,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired),

  /**
     * Shows static min/max labels on the sides of the range slider
     */
  showLabels: PropTypes.bool,

  /**
     * Pass `true` to displays an extra input control for direct manipulation.
     * Pass `'inputWithPopover'` to only show the input but show the range in a dropdown.
     */
  showInput: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.oneOf(["inputWithPopover"])]),

  /**
     * Modifies the number of tick marks and at what interval
     */
  tickInterval: PropTypes.number,

  /**
     * Specified ticks at specified values
     */
  ticks: PropTypes.arrayOf(PropTypes.shape({
    value: PropTypes.number.isRequired,
    label: PropTypes.node.isRequired
  }).isRequired),

  /**
     * Creates an input group with element(s) coming before input.  Will only show if `showInput = inputWithPopover`.
     * `string` | `ReactElement` or an array of these
     */
  prepend: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),

  /**
     * Creates an input group with element(s) coming after input. Will only show if `showInput = inputWithPopover`.
     * `string` | `ReactElement` or an array of these
     */
  append: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),

  /**
     *  Intended to be uses with aria attributes. Some attributes may be overwritten.
     */
  minInputProps: PropTypes.any,

  /**
     *  Intended to be uses with aria attributes. Some attributes may be overwritten.
     */
  maxInputProps: PropTypes.any,

  /**
     *  Creates a draggble highlighted range area
     */
  isDraggable: PropTypes.bool
};