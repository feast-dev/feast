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
EuiAbsoluteTab.propTypes = {
  dateFormat: PropTypes.string.isRequired,
  timeFormat: PropTypes.string.isRequired,
  locale: PropTypes.any,
  value: PropTypes.string.isRequired,
  onChange: PropTypes.func.isRequired,
  roundUp: PropTypes.bool.isRequired,
  position: PropTypes.oneOf(["start", "end"]).isRequired,
  utcOffset: PropTypes.number
};