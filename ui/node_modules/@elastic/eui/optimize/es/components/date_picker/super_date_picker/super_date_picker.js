import _extends from "@babel/runtime/helpers/extends";
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
import moment from 'moment'; // eslint-disable-line import/named

import dateMath from '@elastic/datemath';
import { EuiI18nConsumer } from '../../context';
import { EuiDatePickerRange } from '../date_picker_range';
import { EuiFormControlLayout } from '../../form';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { prettyDuration, showPrettyDuration, commonDurationRanges } from './pretty_duration';
import { AsyncInterval } from './async_interval';
import { EuiSuperUpdateButton } from './super_update_button';
import { EuiQuickSelectPopover } from './quick_select_popover/quick_select_popover';
import { EuiDatePopoverButton } from './date_popover/date_popover_button';
import { EuiAutoRefresh, EuiAutoRefreshButton } from '../auto_refresh/auto_refresh';
import { jsx as ___EmotionJSX } from "@emotion/react";
export { prettyDuration, commonDurationRanges };

function isRangeInvalid(start, end) {
  if (start === 'now' && end === 'now') {
    return true;
  }

  var startMoment = dateMath.parse(start);
  var endMoment = dateMath.parse(end, {
    roundUp: true
  });
  var isInvalid = !startMoment || !endMoment || !startMoment.isValid() || !endMoment.isValid() || !moment(startMoment).isValid() || !moment(endMoment).isValid() || startMoment.isAfter(endMoment);
  return isInvalid;
}

export var EuiSuperDatePicker = /*#__PURE__*/function (_Component) {
  _inherits(EuiSuperDatePicker, _Component);

  var _super = _createSuper(EuiSuperDatePicker);

  function EuiSuperDatePicker() {
    var _this;

    _classCallCheck(this, EuiSuperDatePicker);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "asyncInterval", void 0);

    _defineProperty(_assertThisInitialized(_this), "state", {
      prevProps: {
        start: _this.props.start,
        end: _this.props.end
      },
      start: _this.props.start,
      end: _this.props.end,
      isInvalid: isRangeInvalid(_this.props.start, _this.props.end),
      hasChanged: false,
      showPrettyDuration: showPrettyDuration(_this.props.start, _this.props.end, _this.props.commonlyUsedRanges),
      isStartDatePopoverOpen: false,
      isEndDatePopoverOpen: false
    });

    _defineProperty(_assertThisInitialized(_this), "setTime", function (_ref) {
      var end = _ref.end,
          start = _ref.start;
      var isInvalid = isRangeInvalid(start, end);

      _this.setState({
        start: start,
        end: end,
        isInvalid: isInvalid,
        hasChanged: !(_this.state.prevProps.start === start && _this.state.prevProps.end === end)
      });

      if (!_this.props.showUpdateButton) {
        _this.props.onTimeChange({
          start: start,
          end: end,
          isQuickSelection: false,
          isInvalid: isInvalid
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "componentDidMount", function () {
      if (!_this.props.isPaused) {
        _this.startInterval(_this.props.refreshInterval);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "componentDidUpdate", function () {
      _this.stopInterval();

      if (!_this.props.isPaused) {
        _this.startInterval(_this.props.refreshInterval);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "componentWillUnmount", function () {
      _this.stopInterval();
    });

    _defineProperty(_assertThisInitialized(_this), "setStart", function (start) {
      _this.setTime({
        start: start,
        end: _this.state.end
      });
    });

    _defineProperty(_assertThisInitialized(_this), "setEnd", function (end) {
      _this.setTime({
        start: _this.state.start,
        end: end
      });
    });

    _defineProperty(_assertThisInitialized(_this), "applyTime", function () {
      _this.props.onTimeChange({
        start: _this.state.start,
        end: _this.state.end,
        isQuickSelection: false,
        isInvalid: false
      });
    });

    _defineProperty(_assertThisInitialized(_this), "applyQuickTime", function (_ref2) {
      var start = _ref2.start,
          end = _ref2.end;

      _this.setState({
        showPrettyDuration: showPrettyDuration(start, end, commonDurationRanges)
      });

      _this.props.onTimeChange({
        start: start,
        end: end,
        isQuickSelection: true,
        isInvalid: false
      });
    });

    _defineProperty(_assertThisInitialized(_this), "hidePrettyDuration", function () {
      _this.setState({
        showPrettyDuration: false,
        isStartDatePopoverOpen: true
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onStartDatePopoverToggle", function () {
      _this.setState(function (prevState) {
        return {
          isStartDatePopoverOpen: !prevState.isStartDatePopoverOpen
        };
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onStartDatePopoverClose", function () {
      _this.setState({
        isStartDatePopoverOpen: false
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onEndDatePopoverToggle", function () {
      _this.setState(function (prevState) {
        return {
          isEndDatePopoverOpen: !prevState.isEndDatePopoverOpen
        };
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onEndDatePopoverClose", function () {
      _this.setState({
        isEndDatePopoverOpen: false
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onRefreshChange", function (_ref3) {
      var refreshInterval = _ref3.refreshInterval,
          isPaused = _ref3.isPaused;

      _this.stopInterval();

      if (!isPaused) {
        _this.startInterval(refreshInterval);
      }

      if (_this.props.onRefreshChange) {
        _this.props.onRefreshChange({
          refreshInterval: refreshInterval,
          isPaused: isPaused
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "stopInterval", function () {
      if (_this.asyncInterval) {
        _this.asyncInterval.stop();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "startInterval", function (refreshInterval) {
      var onRefresh = _this.props.onRefresh;

      if (onRefresh) {
        var handler = function handler() {
          var _this$props = _this.props,
              start = _this$props.start,
              end = _this$props.end;
          onRefresh({
            start: start,
            end: end,
            refreshInterval: refreshInterval
          });
        };

        _this.asyncInterval = new AsyncInterval(handler, refreshInterval);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "renderDatePickerRange", function () {
      var _this$state = _this.state,
          end = _this$state.end,
          hasChanged = _this$state.hasChanged,
          isEndDatePopoverOpen = _this$state.isEndDatePopoverOpen,
          isInvalid = _this$state.isInvalid,
          isStartDatePopoverOpen = _this$state.isStartDatePopoverOpen,
          showPrettyDuration = _this$state.showPrettyDuration,
          start = _this$state.start;
      var _this$props2 = _this.props,
          commonlyUsedRanges = _this$props2.commonlyUsedRanges,
          dateFormat = _this$props2.dateFormat,
          isDisabled = _this$props2.isDisabled,
          locale = _this$props2.locale,
          timeFormat = _this$props2.timeFormat,
          utcOffset = _this$props2.utcOffset,
          compressed = _this$props2.compressed;

      if (showPrettyDuration && !isStartDatePopoverOpen && !isEndDatePopoverOpen) {
        return ___EmotionJSX(EuiDatePickerRange, {
          className: "euiDatePickerRange--inGroup",
          iconType: false,
          isCustom: true,
          startDateControl: ___EmotionJSX("div", null),
          endDateControl: ___EmotionJSX("div", null)
        }, ___EmotionJSX("button", {
          className: classNames('euiSuperDatePicker__prettyFormat', {
            'euiSuperDatePicker__prettyFormat--disabled': isDisabled
          }),
          "data-test-subj": "superDatePickerShowDatesButton",
          disabled: isDisabled,
          onClick: _this.hidePrettyDuration
        }, prettyDuration(start, end, commonlyUsedRanges, dateFormat)));
      }

      return ___EmotionJSX(EuiI18nConsumer, null, function (_ref4) {
        var contextLocale = _ref4.locale;
        return ___EmotionJSX(EuiDatePickerRange, {
          className: "euiDatePickerRange--inGroup",
          iconType: false,
          isCustom: true,
          startDateControl: ___EmotionJSX(EuiDatePopoverButton, {
            className: "euiSuperDatePicker__startPopoverButton",
            compressed: compressed,
            position: "start",
            needsUpdating: hasChanged,
            isInvalid: isInvalid,
            isDisabled: isDisabled,
            onChange: _this.setStart,
            value: start,
            dateFormat: dateFormat,
            utcOffset: utcOffset,
            timeFormat: timeFormat,
            locale: locale || contextLocale,
            isOpen: _this.state.isStartDatePopoverOpen,
            onPopoverToggle: _this.onStartDatePopoverToggle,
            onPopoverClose: _this.onStartDatePopoverClose
          }),
          endDateControl: ___EmotionJSX(EuiDatePopoverButton, {
            position: "end",
            compressed: compressed,
            needsUpdating: hasChanged,
            isInvalid: isInvalid,
            isDisabled: isDisabled,
            onChange: _this.setEnd,
            value: end,
            dateFormat: dateFormat,
            utcOffset: utcOffset,
            timeFormat: timeFormat,
            locale: locale || contextLocale,
            roundUp: true,
            isOpen: _this.state.isEndDatePopoverOpen,
            onPopoverToggle: _this.onEndDatePopoverToggle,
            onPopoverClose: _this.onEndDatePopoverClose
          })
        });
      });
    });

    _defineProperty(_assertThisInitialized(_this), "handleClickUpdateButton", function () {
      if (!_this.state.hasChanged && _this.props.onRefresh) {
        var _this$props3 = _this.props,
            start = _this$props3.start,
            end = _this$props3.end,
            refreshInterval = _this$props3.refreshInterval;

        _this.props.onRefresh({
          start: start,
          end: end,
          refreshInterval: refreshInterval
        });
      } else {
        _this.applyTime();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "renderUpdateButton", function () {
      var _this$props4 = _this.props,
          isLoading = _this$props4.isLoading,
          isDisabled = _this$props4.isDisabled,
          updateButtonProps = _this$props4.updateButtonProps,
          showUpdateButton = _this$props4.showUpdateButton,
          compressed = _this$props4.compressed;
      if (!showUpdateButton) return null;
      return ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, ___EmotionJSX(EuiSuperUpdateButton, _extends({
        needsUpdate: _this.state.hasChanged,
        showTooltip: !_this.state.isStartDatePopoverOpen && !_this.state.isEndDatePopoverOpen,
        isLoading: isLoading,
        isDisabled: isDisabled || _this.state.isInvalid,
        onClick: _this.handleClickUpdateButton,
        "data-test-subj": "superDatePickerApplyTimeButton",
        size: compressed ? 's' : 'm',
        iconOnly: showUpdateButton === 'iconOnly'
      }, updateButtonProps)));
    });

    return _this;
  }

  _createClass(EuiSuperDatePicker, [{
    key: "render",
    value: function render() {
      var _this$props5 = this.props,
          commonlyUsedRanges = _this$props5.commonlyUsedRanges,
          customQuickSelectPanels = _this$props5.customQuickSelectPanels,
          dateFormat = _this$props5.dateFormat,
          end = _this$props5.end,
          isAutoRefreshOnly = _this$props5.isAutoRefreshOnly,
          isDisabled = _this$props5.isDisabled,
          isPaused = _this$props5.isPaused,
          onRefreshChange = _this$props5.onRefreshChange,
          recentlyUsedRanges = _this$props5.recentlyUsedRanges,
          refreshInterval = _this$props5.refreshInterval,
          showUpdateButton = _this$props5.showUpdateButton,
          start = _this$props5.start,
          dataTestSubj = _this$props5['data-test-subj'],
          _width = _this$props5.width,
          isQuickSelectOnly = _this$props5.isQuickSelectOnly,
          compressed = _this$props5.compressed; // Force reduction in width if showing quick select only

      var width = isQuickSelectOnly ? 'auto' : _width;
      var autoRefreshAppend = !isPaused ? ___EmotionJSX(EuiAutoRefreshButton, {
        className: "euiFormControlLayout__append",
        refreshInterval: refreshInterval,
        isDisabled: isDisabled,
        isPaused: isPaused,
        onRefreshChange: this.onRefreshChange,
        shortHand: true
      }) : undefined;

      var quickSelect = ___EmotionJSX(EuiQuickSelectPopover, {
        applyRefreshInterval: onRefreshChange ? this.onRefreshChange : undefined,
        applyTime: this.applyQuickTime,
        commonlyUsedRanges: commonlyUsedRanges,
        customQuickSelectPanels: customQuickSelectPanels,
        dateFormat: dateFormat,
        end: end,
        isDisabled: isDisabled,
        isPaused: isPaused,
        recentlyUsedRanges: recentlyUsedRanges,
        refreshInterval: refreshInterval,
        start: start
      });

      var flexWrapperClasses = classNames('euiSuperDatePicker__flexWrapper', {
        'euiSuperDatePicker__flexWrapper--noUpdateButton': !showUpdateButton,
        'euiSuperDatePicker__flexWrapper--isAutoRefreshOnly': isAutoRefreshOnly,
        'euiSuperDatePicker__flexWrapper--isQuickSelectOnly': isQuickSelectOnly,
        'euiSuperDatePicker__flexWrapper--fullWidth': width === 'full',
        'euiSuperDatePicker__flexWrapper--autoWidth': width === 'auto'
      });
      return ___EmotionJSX(EuiFlexGroup, {
        gutterSize: "s",
        responsive: false,
        className: flexWrapperClasses
      }, isAutoRefreshOnly && onRefreshChange ? ___EmotionJSX(EuiFlexItem, null, ___EmotionJSX(EuiAutoRefresh, {
        isPaused: isPaused,
        refreshInterval: refreshInterval,
        onRefreshChange: onRefreshChange,
        fullWidth: width === 'full',
        compressed: compressed,
        isDisabled: isDisabled,
        "data-test-subj": dataTestSubj
      })) : ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiFlexItem, null, ___EmotionJSX(EuiFormControlLayout, {
        className: "euiSuperDatePicker",
        compressed: compressed,
        isDisabled: isDisabled,
        prepend: quickSelect,
        append: autoRefreshAppend,
        "data-test-subj": dataTestSubj
      }, !isQuickSelectOnly && this.renderDatePickerRange())), this.renderUpdateButton()));
    }
  }], [{
    key: "getDerivedStateFromProps",
    value: function getDerivedStateFromProps(nextProps, prevState) {
      if (nextProps.start !== prevState.prevProps.start || nextProps.end !== prevState.prevProps.end) {
        return {
          prevProps: {
            start: nextProps.start,
            end: nextProps.end
          },
          start: nextProps.start,
          end: nextProps.end,
          isInvalid: isRangeInvalid(nextProps.start, nextProps.end),
          hasChanged: false,
          showPrettyDuration: showPrettyDuration(nextProps.start, nextProps.end, nextProps.commonlyUsedRanges)
        };
      }

      return null;
    }
  }]);

  return EuiSuperDatePicker;
}(Component);

_defineProperty(EuiSuperDatePicker, "defaultProps", {
  commonlyUsedRanges: commonDurationRanges,
  dateFormat: 'MMM D, YYYY @ HH:mm:ss.SSS',
  end: 'now',
  isAutoRefreshOnly: false,
  isDisabled: false,
  isPaused: true,
  recentlyUsedRanges: [],
  refreshInterval: 1000,
  showUpdateButton: true,
  start: 'now-15m',
  timeFormat: 'HH:mm',
  width: 'restricted'
});