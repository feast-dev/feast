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
import { EuiI18n } from '../../i18n';
import { keysOf } from '../../common';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { EuiSelect, EuiFieldNumber, EuiFormLabel, EuiSwitch } from '../../form';
import { htmlIdGenerator } from '../../../services';
import { EuiScreenReaderOnly } from '../../accessibility';
import { timeUnits, timeUnitsPlural } from '../super_date_picker/time_units';
import { jsx as ___EmotionJSX } from "@emotion/react";
var refreshUnitsOptions = keysOf(timeUnits).filter(function (timeUnit) {
  return timeUnit === 'h' || timeUnit === 'm' || timeUnit === 's';
}).map(function (timeUnit) {
  return {
    value: timeUnit,
    text: timeUnitsPlural[timeUnit]
  };
});
var MILLISECONDS_IN_SECOND = 1000;
var MILLISECONDS_IN_MINUTE = MILLISECONDS_IN_SECOND * 60;
var MILLISECONDS_IN_HOUR = MILLISECONDS_IN_MINUTE * 60;

function fromMilliseconds(milliseconds) {
  var round = function round(value) {
    return parseFloat(value.toFixed(2));
  };

  if (milliseconds > MILLISECONDS_IN_HOUR) {
    return {
      units: 'h',
      value: round(milliseconds / MILLISECONDS_IN_HOUR)
    };
  }

  if (milliseconds > MILLISECONDS_IN_MINUTE) {
    return {
      units: 'm',
      value: round(milliseconds / MILLISECONDS_IN_MINUTE)
    };
  }

  return {
    units: 's',
    value: round(milliseconds / MILLISECONDS_IN_SECOND)
  };
}

function toMilliseconds(units, value) {
  switch (units) {
    case 'h':
      return Math.round(value * MILLISECONDS_IN_HOUR);

    case 'm':
      return Math.round(value * MILLISECONDS_IN_MINUTE);

    case 's':
    default:
      return Math.round(value * MILLISECONDS_IN_SECOND);
  }
}

export var EuiRefreshInterval = /*#__PURE__*/function (_Component) {
  _inherits(EuiRefreshInterval, _Component);

  var _super = _createSuper(EuiRefreshInterval);

  function EuiRefreshInterval() {
    var _this;

    _classCallCheck(this, EuiRefreshInterval);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "state", fromMilliseconds(_this.props.refreshInterval || 0));

    _defineProperty(_assertThisInitialized(_this), "generateId", htmlIdGenerator());

    _defineProperty(_assertThisInitialized(_this), "legendId", _this.generateId());

    _defineProperty(_assertThisInitialized(_this), "refreshSelectionId", _this.generateId());

    _defineProperty(_assertThisInitialized(_this), "onValueChange", function (event) {
      var sanitizedValue = parseFloat(event.target.value);

      _this.setState({
        value: isNaN(sanitizedValue) ? '' : sanitizedValue
      }, _this.applyRefreshInterval);
    });

    _defineProperty(_assertThisInitialized(_this), "onUnitsChange", function (event) {
      _this.setState({
        units: event.target.value
      }, _this.applyRefreshInterval);
    });

    _defineProperty(_assertThisInitialized(_this), "startRefresh", function () {
      var onRefreshChange = _this.props.onRefreshChange;
      var _this$state = _this.state,
          value = _this$state.value,
          units = _this$state.units;

      if (value !== '' && value > 0 && onRefreshChange !== undefined) {
        onRefreshChange({
          refreshInterval: toMilliseconds(units, value),
          isPaused: false
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "handleKeyDown", function (_ref) {
      var key = _ref.key;

      if (key === 'Enter') {
        _this.startRefresh();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "applyRefreshInterval", function () {
      var _this$props = _this.props,
          onRefreshChange = _this$props.onRefreshChange,
          isPaused = _this$props.isPaused;
      var _this$state2 = _this.state,
          units = _this$state2.units,
          value = _this$state2.value;

      if (value === '') {
        return;
      }

      if (!onRefreshChange) {
        return;
      }

      var refreshInterval = toMilliseconds(units, value);
      onRefreshChange({
        refreshInterval: refreshInterval,
        isPaused: refreshInterval <= 0 ? true : !!isPaused
      });
    });

    _defineProperty(_assertThisInitialized(_this), "toggleRefresh", function () {
      var _this$props2 = _this.props,
          onRefreshChange = _this$props2.onRefreshChange,
          isPaused = _this$props2.isPaused;
      var _this$state3 = _this.state,
          units = _this$state3.units,
          value = _this$state3.value;

      if (!onRefreshChange || value === '') {
        return;
      }

      onRefreshChange({
        refreshInterval: toMilliseconds(units, value),
        isPaused: !isPaused
      });
    });

    return _this;
  }

  _createClass(EuiRefreshInterval, [{
    key: "render",
    value: function render() {
      var isPaused = this.props.isPaused;
      var _this$state4 = this.state,
          value = _this$state4.value,
          units = _this$state4.units;
      var options = refreshUnitsOptions.find(function (_ref2) {
        var value = _ref2.value;
        return value === units;
      });
      var optionText = options ? options.text : '';
      var fullDescription = isPaused ? ___EmotionJSX(EuiI18n, {
        token: "euiRefreshInterval.fullDescriptionOff",
        default: "Refresh is off, interval set to {optionValue} {optionText}.",
        values: {
          optionValue: value,
          optionText: optionText
        }
      }) : ___EmotionJSX(EuiI18n, {
        token: "euiRefreshInterval.fullDescriptionOn",
        default: "Refresh is on, interval set to {optionValue} {optionText}.",
        values: {
          optionValue: value,
          optionText: optionText
        }
      });
      return ___EmotionJSX("fieldset", null, ___EmotionJSX(EuiFlexGroup, {
        alignItems: "center",
        gutterSize: "s",
        responsive: false,
        wrap: true
      }, ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, ___EmotionJSX(EuiSwitch, {
        "data-test-subj": "superDatePickerToggleRefreshButton",
        "aria-describedby": this.refreshSelectionId,
        checked: !isPaused,
        onChange: this.toggleRefresh,
        compressed: true,
        label: ___EmotionJSX(EuiFormLabel, {
          type: "legend",
          id: this.legendId
        }, ___EmotionJSX(EuiI18n, {
          token: "euiRefreshInterval.legend",
          default: "Refresh every"
        }))
      })), ___EmotionJSX(EuiFlexItem, {
        style: {
          minWidth: 60
        }
      }, ___EmotionJSX(EuiFieldNumber, {
        compressed: true,
        fullWidth: true,
        value: value,
        onChange: this.onValueChange,
        onKeyDown: this.handleKeyDown,
        isInvalid: !isPaused && (value === '' || value <= 0),
        disabled: isPaused,
        "aria-label": "Refresh interval value",
        "aria-describedby": "".concat(this.refreshSelectionId, " ").concat(this.legendId),
        "data-test-subj": "superDatePickerRefreshIntervalInput"
      })), ___EmotionJSX(EuiFlexItem, {
        style: {
          minWidth: 100
        },
        grow: 2
      }, ___EmotionJSX(EuiSelect, {
        compressed: true,
        fullWidth: true,
        "aria-label": "Refresh interval units",
        "aria-describedby": "".concat(this.refreshSelectionId, " ").concat(this.legendId),
        value: units,
        disabled: isPaused,
        options: refreshUnitsOptions,
        onChange: this.onUnitsChange,
        onKeyDown: this.handleKeyDown,
        "data-test-subj": "superDatePickerRefreshIntervalUnitsSelect"
      }))), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", {
        id: this.refreshSelectionId
      }, fullDescription)));
    }
  }]);

  return EuiRefreshInterval;
}(Component);

_defineProperty(EuiRefreshInterval, "defaultProps", {
  isPaused: true,
  refreshInterval: 1000
});