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
import React, { Component, Fragment } from 'react';
import { EuiButtonEmpty } from '../../../button';
import { EuiIcon } from '../../../icon';
import { EuiPopover } from '../../../popover';
import { EuiTitle } from '../../../title';
import { EuiSpacer } from '../../../spacer';
import { EuiHorizontalRule } from '../../../horizontal_rule';
import { EuiText } from '../../../text';
import { EuiQuickSelect } from './quick_select';
import { EuiCommonlyUsedTimeRanges } from './commonly_used_time_ranges';
import { EuiRecentlyUsed } from './recently_used';
import { EuiRefreshInterval } from '../../auto_refresh/refresh_interval';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiQuickSelectPopover = /*#__PURE__*/function (_Component) {
  _inherits(EuiQuickSelectPopover, _Component);

  var _super = _createSuper(EuiQuickSelectPopover);

  function EuiQuickSelectPopover() {
    var _this;

    _classCallCheck(this, EuiQuickSelectPopover);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "state", {
      isOpen: false
    });

    _defineProperty(_assertThisInitialized(_this), "closePopover", function () {
      _this.setState({
        isOpen: false
      });
    });

    _defineProperty(_assertThisInitialized(_this), "togglePopover", function () {
      _this.setState(function (prevState) {
        return {
          isOpen: !prevState.isOpen
        };
      });
    });

    _defineProperty(_assertThisInitialized(_this), "applyTime", function (_ref) {
      var start = _ref.start,
          end = _ref.end,
          quickSelect = _ref.quickSelect,
          _ref$keepPopoverOpen = _ref.keepPopoverOpen,
          keepPopoverOpen = _ref$keepPopoverOpen === void 0 ? false : _ref$keepPopoverOpen;

      _this.props.applyTime({
        start: start,
        end: end
      });

      if (quickSelect) {
        _this.setState({
          prevQuickSelect: quickSelect
        });
      }

      if (!keepPopoverOpen) {
        _this.closePopover();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "renderDateTimeSections", function () {
      var _this$props = _this.props,
          commonlyUsedRanges = _this$props.commonlyUsedRanges,
          dateFormat = _this$props.dateFormat,
          end = _this$props.end,
          recentlyUsedRanges = _this$props.recentlyUsedRanges,
          start = _this$props.start;
      var prevQuickSelect = _this.state.prevQuickSelect;
      return ___EmotionJSX(Fragment, null, ___EmotionJSX(EuiQuickSelect, {
        applyTime: _this.applyTime,
        start: start,
        end: end,
        prevQuickSelect: prevQuickSelect
      }), commonlyUsedRanges.length > 0 && ___EmotionJSX(EuiHorizontalRule, {
        margin: "s"
      }), ___EmotionJSX(EuiCommonlyUsedTimeRanges, {
        applyTime: _this.applyTime,
        commonlyUsedRanges: commonlyUsedRanges
      }), recentlyUsedRanges.length > 0 && ___EmotionJSX(EuiHorizontalRule, {
        margin: "s"
      }), ___EmotionJSX(EuiRecentlyUsed, {
        applyTime: _this.applyTime,
        commonlyUsedRanges: commonlyUsedRanges,
        dateFormat: dateFormat,
        recentlyUsedRanges: recentlyUsedRanges
      }), _this.renderCustomQuickSelectPanels());
    });

    _defineProperty(_assertThisInitialized(_this), "renderCustomQuickSelectPanels", function () {
      var customQuickSelectPanels = _this.props.customQuickSelectPanels;

      if (!customQuickSelectPanels) {
        return null;
      }

      return customQuickSelectPanels.map(function (_ref2) {
        var title = _ref2.title,
            content = _ref2.content;
        return ___EmotionJSX(Fragment, {
          key: title
        }, ___EmotionJSX(EuiHorizontalRule, {
          margin: "s"
        }), ___EmotionJSX(EuiTitle, {
          size: "xxxs"
        }, ___EmotionJSX("span", null, title)), ___EmotionJSX(EuiSpacer, {
          size: "xs"
        }), ___EmotionJSX(EuiText, {
          size: "s",
          className: "euiQuickSelectPopover__section"
        }, /*#__PURE__*/React.cloneElement(content, {
          applyTime: _this.applyTime
        })));
      });
    });

    return _this;
  }

  _createClass(EuiQuickSelectPopover, [{
    key: "render",
    value: function render() {
      var _this$props2 = this.props,
          applyRefreshInterval = _this$props2.applyRefreshInterval,
          isDisabled = _this$props2.isDisabled,
          isPaused = _this$props2.isPaused,
          refreshInterval = _this$props2.refreshInterval;
      var isOpen = this.state.isOpen;

      var quickSelectButton = ___EmotionJSX(EuiButtonEmpty, {
        className: "euiFormControlLayout__prepend",
        textProps: {
          className: 'euiQuickSelectPopover__buttonText'
        },
        onClick: this.togglePopover,
        "aria-label": "Date quick select",
        size: "xs",
        iconType: "arrowDown",
        iconSide: "right",
        isDisabled: isDisabled,
        "data-test-subj": "superDatePickerToggleQuickMenuButton"
      }, ___EmotionJSX(EuiIcon, {
        type: "calendar"
      }));

      return ___EmotionJSX(EuiPopover, {
        button: quickSelectButton,
        isOpen: isOpen,
        closePopover: this.closePopover,
        anchorPosition: "downLeft",
        anchorClassName: "euiQuickSelectPopover__anchor"
      }, ___EmotionJSX("div", {
        className: "euiQuickSelectPopover__content",
        "data-test-subj": "superDatePickerQuickMenu"
      }, this.renderDateTimeSections(), applyRefreshInterval && ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiHorizontalRule, {
        margin: "s"
      }), ___EmotionJSX(EuiRefreshInterval, {
        onRefreshChange: applyRefreshInterval,
        isPaused: isPaused,
        refreshInterval: refreshInterval
      }))));
    }
  }]);

  return EuiQuickSelectPopover;
}(Component);