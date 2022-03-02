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
import React, { Component, Fragment } from 'react';
import PropTypes from "prop-types";
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
EuiQuickSelectPopover.propTypes = {
  applyRefreshInterval: PropTypes.func,
  applyTime: PropTypes.func.isRequired,
  commonlyUsedRanges: PropTypes.arrayOf(PropTypes.shape({
    end: PropTypes.oneOfType([PropTypes.oneOf(["now"]), PropTypes.string.isRequired]).isRequired,
    label: PropTypes.string,
    start: PropTypes.oneOfType([PropTypes.oneOf(["now"]), PropTypes.string.isRequired]).isRequired
  }).isRequired).isRequired,
  customQuickSelectPanels: PropTypes.arrayOf(PropTypes.shape({
    title: PropTypes.string.isRequired,
    content: PropTypes.element.isRequired
  }).isRequired),
  dateFormat: PropTypes.string.isRequired,
  end: PropTypes.string.isRequired,
  isDisabled: PropTypes.bool.isRequired,
  isPaused: PropTypes.bool.isRequired,
  recentlyUsedRanges: PropTypes.arrayOf(PropTypes.shape({
    end: PropTypes.oneOfType([PropTypes.oneOf(["now"]), PropTypes.string.isRequired]).isRequired,
    label: PropTypes.string,
    start: PropTypes.oneOfType([PropTypes.oneOf(["now"]), PropTypes.string.isRequired]).isRequired
  }).isRequired).isRequired,
  refreshInterval: PropTypes.number.isRequired,
  start: PropTypes.string.isRequired
};