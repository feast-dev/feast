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

EuiRefreshInterval.propTypes = {
  /**
     * Is refresh paused or running.
     */
  isPaused: PropTypes.bool,

  /**
     * Refresh interval in milliseconds.
     */
  refreshInterval: PropTypes.number,

  /**
     * Passes back the updated state of `isPaused` and `refreshInterval`.
     */
  onRefreshChange: PropTypes.func.isRequired
};