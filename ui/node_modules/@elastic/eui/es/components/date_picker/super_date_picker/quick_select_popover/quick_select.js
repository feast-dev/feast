function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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
import moment from 'moment';
import dateMath from '@elastic/datemath';
import { htmlIdGenerator } from '../../../../services';
import { EuiButton, EuiButtonIcon } from '../../../button';
import { EuiFlexGroup, EuiFlexItem } from '../../../flex';
import { EuiSpacer } from '../../../spacer';
import { EuiSelect, EuiFieldNumber } from '../../../form';
import { EuiToolTip } from '../../../tool_tip';
import { EuiI18n } from '../../../i18n';
import { timeUnits } from '../time_units';
import { EuiScreenReaderOnly } from '../../../accessibility';
import { keysOf } from '../../../common';
import { parseTimeParts } from './quick_select_utils';
import { jsx as ___EmotionJSX } from "@emotion/react";
var LAST = 'last';
var NEXT = 'next';
var timeTenseOptions = [{
  value: LAST,
  text: 'Last'
}, {
  value: NEXT,
  text: 'Next'
}];
var timeUnitsOptions = keysOf(timeUnits).map(function (key) {
  return {
    value: key,
    text: "".concat(timeUnits[key], "s")
  };
});
export var EuiQuickSelect = /*#__PURE__*/function (_Component) {
  _inherits(EuiQuickSelect, _Component);

  var _super = _createSuper(EuiQuickSelect);

  function EuiQuickSelect(props) {
    var _this;

    _classCallCheck(this, EuiQuickSelect);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "generateId", htmlIdGenerator());

    _defineProperty(_assertThisInitialized(_this), "timeSelectionId", _this.generateId());

    _defineProperty(_assertThisInitialized(_this), "legendId", _this.generateId());

    _defineProperty(_assertThisInitialized(_this), "onTimeTenseChange", function (event) {
      _this.setState({
        timeTense: event.target.value
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onTimeValueChange", function (event) {
      var sanitizedValue = parseInt(event.target.value, 10);

      _this.setState({
        timeValue: isNaN(sanitizedValue) ? 0 : sanitizedValue
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onTimeUnitsChange", function (event) {
      _this.setState({
        timeUnits: event.target.value
      });
    });

    _defineProperty(_assertThisInitialized(_this), "handleKeyDown", function (_ref) {
      var key = _ref.key;

      if (key === 'Enter') {
        _this.applyQuickSelect();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "applyQuickSelect", function () {
      var _this$state = _this.state,
          timeTense = _this$state.timeTense,
          timeValue = _this$state.timeValue,
          timeUnits = _this$state.timeUnits;

      if (timeTense === NEXT) {
        _this.props.applyTime({
          start: 'now',
          end: "now+".concat(timeValue).concat(timeUnits),
          quickSelect: _objectSpread({}, _this.state)
        });

        return;
      }

      _this.props.applyTime({
        start: "now-".concat(timeValue).concat(timeUnits),
        end: 'now',
        quickSelect: _objectSpread({}, _this.state)
      });
    });

    _defineProperty(_assertThisInitialized(_this), "getBounds", function () {
      var startMoment = dateMath.parse(_this.props.start);
      var endMoment = dateMath.parse(_this.props.end, {
        roundUp: true
      });
      return {
        min: startMoment && startMoment.isValid() ? startMoment : moment().subtract(15, 'minute'),
        max: endMoment && endMoment.isValid() ? endMoment : moment()
      };
    });

    _defineProperty(_assertThisInitialized(_this), "stepForward", function () {
      var _this$getBounds = _this.getBounds(),
          min = _this$getBounds.min,
          max = _this$getBounds.max;

      var diff = max.diff(min);

      _this.props.applyTime({
        start: moment(max).add(1, 'ms').toISOString(),
        end: moment(max).add(diff + 1, 'ms').toISOString(),
        keepPopoverOpen: true
      });
    });

    _defineProperty(_assertThisInitialized(_this), "stepBackward", function () {
      var _this$getBounds2 = _this.getBounds(),
          min = _this$getBounds2.min,
          max = _this$getBounds2.max;

      var diff = max.diff(min);

      _this.props.applyTime({
        start: moment(min).subtract(diff + 1, 'ms').toISOString(),
        end: moment(min).subtract(1, 'ms').toISOString(),
        keepPopoverOpen: true
      });
    });

    var _parseTimeParts = parseTimeParts(props.start, props.end),
        timeTenseDefault = _parseTimeParts.timeTense,
        timeUnitsDefault = _parseTimeParts.timeUnits,
        timeValueDefault = _parseTimeParts.timeValue;

    _this.state = {
      timeTense: props.prevQuickSelect && props.prevQuickSelect.timeTense ? props.prevQuickSelect.timeTense : timeTenseDefault,
      timeValue: props.prevQuickSelect && props.prevQuickSelect.timeValue ? props.prevQuickSelect.timeValue : timeValueDefault,
      timeUnits: props.prevQuickSelect && props.prevQuickSelect.timeUnits ? props.prevQuickSelect.timeUnits : timeUnitsDefault
    };
    return _this;
  }

  _createClass(EuiQuickSelect, [{
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$state2 = this.state,
          timeTense = _this$state2.timeTense,
          timeValue = _this$state2.timeValue,
          timeUnits = _this$state2.timeUnits;
      var matchedTimeUnit = timeUnitsOptions.find(function (_ref2) {
        var value = _ref2.value;
        return value === timeUnits;
      });
      var timeUnit = matchedTimeUnit ? matchedTimeUnit.text : '';
      return ___EmotionJSX("fieldset", null, ___EmotionJSX(EuiI18n, {
        token: "euiQuickSelect.legendText",
        default: "Quick select a time range"
      }, function (legendText) {
        return (// Legend needs to be the first thing in a fieldset, but we want the visible title within the flex.
          // So we hide it, but allow screen readers to see it
          ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("legend", {
            id: _this2.legendId,
            className: "euiFormLabel"
          }, legendText))
        );
      }), ___EmotionJSX(EuiFlexGroup, {
        responsive: false,
        alignItems: "center",
        justifyContent: "spaceBetween",
        gutterSize: "s"
      }, ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, ___EmotionJSX(EuiI18n, {
        token: "euiQuickSelect.quickSelectTitle",
        default: "Quick select"
      }, function (quickSelectTitle) {
        return ___EmotionJSX("div", {
          "aria-hidden": true,
          className: "euiFormLabel"
        }, quickSelectTitle);
      })), ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, ___EmotionJSX(EuiFlexGroup, {
        alignItems: "center",
        gutterSize: "s",
        responsive: false
      }, ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, ___EmotionJSX(EuiI18n, {
        token: "euiQuickSelect.previousLabel",
        default: "Previous time window"
      }, function (previousLabel) {
        return ___EmotionJSX(EuiToolTip, {
          content: previousLabel
        }, ___EmotionJSX(EuiButtonIcon, {
          "aria-label": previousLabel,
          iconType: "arrowLeft",
          onClick: _this2.stepBackward
        }));
      })), ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, ___EmotionJSX(EuiI18n, {
        token: "euiQuickSelect.nextLabel",
        default: "Next time window"
      }, function (nextLabel) {
        return ___EmotionJSX(EuiToolTip, {
          content: nextLabel
        }, ___EmotionJSX(EuiButtonIcon, {
          "aria-label": nextLabel,
          iconType: "arrowRight",
          onClick: _this2.stepForward
        }));
      }))))), ___EmotionJSX(EuiSpacer, {
        size: "s"
      }), ___EmotionJSX(EuiFlexGroup, {
        gutterSize: "s",
        responsive: false
      }, ___EmotionJSX(EuiFlexItem, null, ___EmotionJSX(EuiI18n, {
        token: "euiQuickSelect.tenseLabel",
        default: "Time tense"
      }, function (tenseLabel) {
        return ___EmotionJSX(EuiSelect, {
          compressed: true,
          onKeyDown: _this2.handleKeyDown,
          "aria-label": tenseLabel,
          "aria-describedby": "".concat(_this2.timeSelectionId, " ").concat(_this2.legendId),
          value: timeTense,
          options: timeTenseOptions,
          onChange: _this2.onTimeTenseChange
        });
      })), ___EmotionJSX(EuiFlexItem, null, ___EmotionJSX(EuiI18n, {
        token: "euiQuickSelect.valueLabel",
        default: "Time value"
      }, function (valueLabel) {
        return ___EmotionJSX(EuiFieldNumber, {
          compressed: true,
          onKeyDown: _this2.handleKeyDown,
          "aria-describedby": "".concat(_this2.timeSelectionId, " ").concat(_this2.legendId),
          "aria-label": valueLabel,
          value: timeValue,
          onChange: _this2.onTimeValueChange
        });
      })), ___EmotionJSX(EuiFlexItem, null, ___EmotionJSX(EuiI18n, {
        token: "euiQuickSelect.unitLabel",
        default: "Time unit"
      }, function (unitLabel) {
        return ___EmotionJSX(EuiSelect, {
          compressed: true,
          onKeyDown: _this2.handleKeyDown,
          "aria-label": unitLabel,
          "aria-describedby": "".concat(_this2.timeSelectionId, " ").concat(_this2.legendId),
          value: timeUnits,
          options: timeUnitsOptions,
          onChange: _this2.onTimeUnitsChange
        });
      })), ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, ___EmotionJSX(EuiButton, {
        "aria-describedby": "".concat(this.timeSelectionId, " ").concat(this.legendId),
        className: "euiQuickSelect__applyButton",
        size: "s",
        onClick: this.applyQuickSelect,
        disabled: timeValue <= 0
      }, ___EmotionJSX(EuiI18n, {
        token: "euiQuickSelect.applyButton",
        default: "Apply"
      })))), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", {
        id: this.timeSelectionId
      }, ___EmotionJSX(EuiI18n, {
        token: "euiQuickSelect.fullDescription",
        default: "Currently set to {timeTense} {timeValue} {timeUnit}.",
        values: {
          timeTense: timeTense,
          timeValue: timeValue,
          timeUnit: timeUnit
        }
      }))));
    }
  }]);

  return EuiQuickSelect;
}(Component);
EuiQuickSelect.propTypes = {
  applyTime: PropTypes.func.isRequired,
  start: PropTypes.string.isRequired,
  end: PropTypes.string.isRequired,
  prevQuickSelect: PropTypes.shape({
    timeTense: PropTypes.string.isRequired,
    timeValue: PropTypes.number.isRequired,
    timeUnits: PropTypes.oneOf(["s", "m", "h", "d", "w", "M", "y"]).isRequired
  })
};