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
import moment from 'moment'; // eslint-disable-line import/named

import dateMath from '@elastic/datemath';
import { EuiDatePicker } from '../../date_picker';
import { EuiFormRow, EuiFieldText, EuiFormLabel } from '../../../form';
import { toSentenceCase } from '../../../../services/string/to_case';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiAbsoluteTab = /*#__PURE__*/function (_Component) {
  _inherits(EuiAbsoluteTab, _Component);

  var _super = _createSuper(EuiAbsoluteTab);

  function EuiAbsoluteTab(props) {
    var _this;

    _classCallCheck(this, EuiAbsoluteTab);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "state", void 0);

    _defineProperty(_assertThisInitialized(_this), "handleChange", function (date, event) {
      var onChange = _this.props.onChange;

      if (date === null) {
        return;
      }

      onChange(date.toISOString(), event);
      var valueAsMoment = moment(date);

      _this.setState({
        valueAsMoment: valueAsMoment,
        textInputValue: valueAsMoment.format(_this.props.dateFormat),
        isTextInvalid: false
      });
    });

    _defineProperty(_assertThisInitialized(_this), "handleTextChange", function (event) {
      var onChange = _this.props.onChange;
      var valueAsMoment = moment(event.target.value, _this.props.dateFormat, true);
      var dateIsValid = valueAsMoment.isValid();

      if (dateIsValid) {
        onChange(valueAsMoment.toISOString(), event);
      }

      _this.setState({
        textInputValue: event.target.value,
        isTextInvalid: !dateIsValid,
        valueAsMoment: dateIsValid ? valueAsMoment : null
      });
    });

    var sentenceCasedPosition = toSentenceCase(props.position);
    var parsedValue = dateMath.parse(props.value, {
      roundUp: props.roundUp
    });

    var _valueAsMoment = parsedValue && parsedValue.isValid() ? parsedValue : moment();

    var textInputValue = _valueAsMoment.locale(_this.props.locale || 'en').format(_this.props.dateFormat);

    _this.state = {
      isTextInvalid: false,
      sentenceCasedPosition: sentenceCasedPosition,
      textInputValue: textInputValue,
      valueAsMoment: _valueAsMoment
    };
    return _this;
  }

  _createClass(EuiAbsoluteTab, [{
    key: "render",
    value: function render() {
      var _this$props = this.props,
          dateFormat = _this$props.dateFormat,
          timeFormat = _this$props.timeFormat,
          locale = _this$props.locale,
          utcOffset = _this$props.utcOffset;
      var _this$state = this.state,
          valueAsMoment = _this$state.valueAsMoment,
          isTextInvalid = _this$state.isTextInvalid,
          textInputValue = _this$state.textInputValue,
          sentenceCasedPosition = _this$state.sentenceCasedPosition;
      return ___EmotionJSX("div", null, ___EmotionJSX(EuiDatePicker, {
        inline: true,
        showTimeSelect: true,
        shadow: false,
        selected: valueAsMoment,
        onChange: this.handleChange,
        dateFormat: dateFormat,
        timeFormat: timeFormat,
        locale: locale,
        utcOffset: utcOffset
      }), ___EmotionJSX(EuiFormRow, {
        className: "euiSuperDatePicker__absoluteDateFormRow",
        isInvalid: isTextInvalid,
        error: isTextInvalid ? "Expected format ".concat(dateFormat) : undefined
      }, ___EmotionJSX(EuiFieldText, {
        compressed: true,
        isInvalid: isTextInvalid,
        value: textInputValue,
        onChange: this.handleTextChange,
        "data-test-subj": 'superDatePickerAbsoluteDateInput',
        prepend: ___EmotionJSX(EuiFormLabel, null, sentenceCasedPosition, " date")
      })));
    }
  }]);

  return EuiAbsoluteTab;
}(Component);