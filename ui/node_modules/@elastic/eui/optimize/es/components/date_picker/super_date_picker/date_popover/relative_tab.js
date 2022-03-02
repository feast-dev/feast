import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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
import dateMath from '@elastic/datemath';
import { toSentenceCase } from '../../../../services/string/to_case';
import { htmlIdGenerator } from '../../../../services';
import { EuiFlexGroup, EuiFlexItem } from '../../../flex';
import { EuiForm, EuiFormRow, EuiSelect, EuiFieldNumber, EuiFieldText, EuiSwitch, EuiFormLabel } from '../../../form';
import { EuiSpacer } from '../../../spacer';
import { timeUnits } from '../time_units';
import { relativeOptions } from '../relative_options';
import { parseRelativeParts, toRelativeStringFromParts } from '../relative_utils';
import { EuiScreenReaderOnly } from '../../../accessibility';
import { EuiI18n } from '../../../i18n';
import { INVALID_DATE } from '../date_modes';
import { EuiPopoverFooter } from '../../../popover';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiRelativeTab = /*#__PURE__*/function (_Component) {
  _inherits(EuiRelativeTab, _Component);

  var _super = _createSuper(EuiRelativeTab);

  function EuiRelativeTab() {
    var _this;

    _classCallCheck(this, EuiRelativeTab);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "state", _objectSpread(_objectSpread({}, parseRelativeParts(_this.props.value)), {}, {
      sentenceCasedPosition: toSentenceCase(_this.props.position)
    }));

    _defineProperty(_assertThisInitialized(_this), "relativeDateInputNumberDescriptionId", htmlIdGenerator()());

    _defineProperty(_assertThisInitialized(_this), "onCountChange", function (event) {
      var sanitizedValue = parseInt(event.target.value, 10);

      _this.setState({
        count: isNaN(sanitizedValue) ? undefined : sanitizedValue
      }, _this.handleChange);
    });

    _defineProperty(_assertThisInitialized(_this), "onUnitChange", function (event) {
      _this.setState({
        unit: event.target.value
      }, _this.handleChange);
    });

    _defineProperty(_assertThisInitialized(_this), "onRoundChange", function (event) {
      _this.setState({
        round: event.target.checked
      }, _this.handleChange);
    });

    _defineProperty(_assertThisInitialized(_this), "handleChange", function () {
      var _this$state = _this.state,
          count = _this$state.count,
          round = _this$state.round,
          roundUnit = _this$state.roundUnit,
          unit = _this$state.unit;
      var onChange = _this.props.onChange;

      if (count === undefined || count < 0) {
        return;
      }

      var date = toRelativeStringFromParts({
        count: count,
        round: round,
        roundUnit: roundUnit,
        unit: unit
      });
      onChange(date);
    });

    return _this;
  }

  _createClass(EuiRelativeTab, [{
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$state2 = this.state,
          count = _this$state2.count,
          unit = _this$state2.unit;
      var invalidDate = this.props.value === INVALID_DATE;
      var invalidValue = count === undefined || count < 0;
      var isInvalid = invalidValue || invalidDate;
      var parsedValue = dateMath.parse(this.props.value, {
        roundUp: this.props.roundUp
      });
      var formattedValue = isInvalid || !parsedValue || !parsedValue.isValid() ? '' : parsedValue.locale(this.props.locale || 'en').format(this.props.dateFormat);

      var getErrorMessage = function getErrorMessage(_ref) {
        var numberInputError = _ref.numberInputError,
            dateInputError = _ref.dateInputError;
        if (invalidValue) return numberInputError;
        if (invalidDate) return dateInputError;
        return null;
      };

      return ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiForm, {
        className: "euiDatePopoverContent__padded"
      }, ___EmotionJSX(EuiFlexGroup, {
        gutterSize: "s",
        responsive: false
      }, ___EmotionJSX(EuiFlexItem, null, ___EmotionJSX(EuiI18n, {
        tokens: ['euiRelativeTab.numberInputError', 'euiRelativeTab.numberInputLabel', 'euiRelativeTab.dateInputError'],
        defaults: ['Must be >= 0', 'Time span amount', 'Must be a valid range']
      }, function (_ref2) {
        var _ref3 = _slicedToArray(_ref2, 3),
            numberInputError = _ref3[0],
            numberInputLabel = _ref3[1],
            dateInputError = _ref3[2];

        return ___EmotionJSX(EuiFormRow, {
          isInvalid: isInvalid,
          error: getErrorMessage({
            numberInputError: numberInputError,
            dateInputError: dateInputError
          })
        }, ___EmotionJSX(EuiFieldNumber, {
          compressed: true,
          "aria-label": numberInputLabel,
          "aria-describedby": _this2.relativeDateInputNumberDescriptionId,
          "data-test-subj": 'superDatePickerRelativeDateInputNumber',
          value: count,
          onChange: _this2.onCountChange,
          isInvalid: isInvalid
        }));
      })), ___EmotionJSX(EuiFlexItem, null, ___EmotionJSX(EuiI18n, {
        token: "euiRelativeTab.unitInputLabel",
        default: "Relative time span"
      }, function (unitInputLabel) {
        return ___EmotionJSX(EuiSelect, {
          compressed: true,
          "aria-label": unitInputLabel,
          "data-test-subj": 'superDatePickerRelativeDateInputUnitSelector',
          value: unit,
          options: relativeOptions,
          onChange: _this2.onUnitChange
        });
      }))), ___EmotionJSX(EuiSpacer, {
        size: "s"
      }), ___EmotionJSX(EuiFieldText, {
        compressed: true,
        value: formattedValue,
        readOnly: true,
        prepend: ___EmotionJSX(EuiFormLabel, null, ___EmotionJSX(EuiI18n, {
          token: "euiRelativeTab.relativeDate",
          default: "{position} date",
          values: {
            position: this.state.sentenceCasedPosition
          }
        }))
      }), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", {
        id: this.relativeDateInputNumberDescriptionId
      }, ___EmotionJSX(EuiI18n, {
        token: "euiRelativeTab.fullDescription",
        default: "The unit is changeable. Currently set to {unit}.",
        values: {
          unit: unit
        }
      })))), ___EmotionJSX(EuiPopoverFooter, {
        paddingSize: "s"
      }, ___EmotionJSX(EuiI18n, {
        token: "euiRelativeTab.roundingLabel",
        default: "Round to the {unit}",
        values: {
          unit: timeUnits[unit.substring(0, 1)]
        }
      }, function (roundingLabel) {
        return ___EmotionJSX(EuiSwitch, {
          "data-test-subj": 'superDatePickerRelativeDateRoundSwitch',
          label: roundingLabel,
          checked: _this2.state.round,
          onChange: _this2.onRoundChange
        });
      })));
    }
  }]);

  return EuiRelativeTab;
}(Component);