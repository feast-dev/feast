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
import range from 'lodash/range';
import { isEvenlyDivisibleBy } from '../../../services';
import { EuiRangeLevels, LEVEL_COLORS } from './range_levels';
import { EuiRangeTicks } from './range_ticks';
import { jsx as ___EmotionJSX } from "@emotion/react";
export { LEVEL_COLORS };
export var EuiRangeTrack = /*#__PURE__*/function (_Component) {
  _inherits(EuiRangeTrack, _Component);

  var _super = _createSuper(EuiRangeTrack);

  function EuiRangeTrack() {
    var _this;

    _classCallCheck(this, EuiRangeTrack);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "validateValueIsInStep", function (value) {
      if (value < _this.props.min) {
        throw new Error("The value of ".concat(value, " is lower than the min value of ").concat(_this.props.min, "."));
      }

      if (value > _this.props.max) {
        throw new Error("The value of ".concat(value, " is higher than the max value of ").concat(_this.props.max, "."));
      } // Error out if the value doesn't line up with the sequence of steps


      if (!isEvenlyDivisibleBy(value - _this.props.min, _this.props.step !== undefined ? _this.props.step : 1)) {
        throw new Error("The value of ".concat(value, " is not included in the possible sequence provided by the step of ").concat(_this.props.step, "."));
      } // Return the value if nothing fails


      return value;
    });

    _defineProperty(_assertThisInitialized(_this), "calculateSequence", function (min, max, interval) {
      // Loop from min to max, creating adding values at each interval
      var sequence = range(min, max, interval); // range is non-inclusive of max, so make it inclusive

      if (max % interval === 0 && !sequence.includes(max)) {
        sequence.push(max);
      }

      return sequence;
    });

    _defineProperty(_assertThisInitialized(_this), "calculateTicks", function (min, max, step, tickInterval, customTicks) {
      var ticks;

      if (customTicks) {
        // If custom values were passed, use those for the sequence
        // But make sure they align with the possible sequence
        ticks = customTicks.map(function (tick) {
          return _this.validateValueIsInStep(tick.value);
        });
      } else {
        // If a custom interval was passed, use those for the sequence
        // But make sure they align with the possible sequence
        var interval = tickInterval || step;

        var tickSequence = _this.calculateSequence(min, max, interval);

        ticks = tickSequence.map(function (tick) {
          return _this.validateValueIsInStep(tick);
        });
      } // Error out if there are too many ticks to render


      if (ticks.length > 20) {
        throw new Error("The number of ticks to render is too high (".concat(ticks.length, "), reduce the interval."));
      }

      return ticks;
    });

    return _this;
  }

  _createClass(EuiRangeTrack, [{
    key: "render",
    value: function render() {
      var _this$props = this.props,
          children = _this$props.children,
          disabled = _this$props.disabled,
          max = _this$props.max,
          min = _this$props.min,
          step = _this$props.step,
          showTicks = _this$props.showTicks,
          tickInterval = _this$props.tickInterval,
          ticks = _this$props.ticks,
          levels = _this$props.levels,
          onChange = _this$props.onChange,
          value = _this$props.value,
          compressed = _this$props.compressed,
          rest = _objectWithoutProperties(_this$props, ["children", "disabled", "max", "min", "step", "showTicks", "tickInterval", "ticks", "levels", "onChange", "value", "compressed"]); // TODO: Move these to only re-calculate if no-value props have changed


      this.validateValueIsInStep(max);
      var tickSequence = showTicks === true && this.calculateTicks(min, max, step, tickInterval, ticks);
      var trackClasses = classNames('euiRangeTrack', {
        'euiRangeTrack--disabled': disabled,
        'euiRangeTrack--hasLevels': levels && !!levels.length,
        'euiRangeTrack--hasTicks': tickSequence || ticks,
        'euiRangeTrack--compressed': compressed
      });
      return ___EmotionJSX("div", _extends({
        className: trackClasses
      }, rest), levels && !!levels.length && ___EmotionJSX(EuiRangeLevels, {
        compressed: compressed,
        levels: levels,
        max: max,
        min: min,
        showTicks: showTicks
      }), tickSequence && ___EmotionJSX(EuiRangeTicks, {
        disabled: disabled,
        compressed: compressed,
        onChange: onChange,
        ticks: ticks,
        tickSequence: tickSequence,
        value: value,
        min: min,
        max: max,
        interval: tickInterval || step
      }), children);
    }
  }]);

  return EuiRangeTrack;
}(Component);