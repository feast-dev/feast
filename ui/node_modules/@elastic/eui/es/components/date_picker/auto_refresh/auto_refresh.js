function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useState } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { EuiFieldText } from '../../form';
import { EuiButtonEmpty } from '../../button/button_empty/button_empty';
import { EuiInputPopover, EuiPopover } from '../../popover';
import { useEuiI18n } from '../../i18n';
import { prettyInterval } from '../super_date_picker/pretty_interval';
import { EuiRefreshInterval } from './refresh_interval';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiAutoRefresh = function EuiAutoRefresh(_ref) {
  var className = _ref.className,
      onRefreshChange = _ref.onRefreshChange,
      isDisabled = _ref.isDisabled,
      _ref$isPaused = _ref.isPaused,
      isPaused = _ref$isPaused === void 0 ? true : _ref$isPaused,
      _ref$refreshInterval = _ref.refreshInterval,
      refreshInterval = _ref$refreshInterval === void 0 ? 1000 : _ref$refreshInterval,
      _ref$readOnly = _ref.readOnly,
      readOnly = _ref$readOnly === void 0 ? true : _ref$readOnly,
      rest = _objectWithoutProperties(_ref, ["className", "onRefreshChange", "isDisabled", "isPaused", "refreshInterval", "readOnly"]);

  var classes = classNames('euiAutoRefresh', className);

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isPopoverOpen = _useState2[0],
      setIsPopoverOpen = _useState2[1];

  var autoRefeshLabel = useEuiI18n('euiAutoRefresh.autoRefreshLabel', 'Auto refresh');
  return ___EmotionJSX(EuiInputPopover, {
    className: classes,
    fullWidth: rest.fullWidth,
    input: ___EmotionJSX(EuiFieldText, _extends({
      "aria-label": autoRefeshLabel,
      onClick: function onClick() {
        return setIsPopoverOpen(function (isOpen) {
          return !isOpen;
        });
      },
      prepend: ___EmotionJSX(EuiButtonEmpty, {
        className: "euiFormControlLayout__prepend",
        onClick: function onClick() {
          return setIsPopoverOpen(function (isOpen) {
            return !isOpen;
          });
        },
        size: "s",
        color: "text",
        iconType: "timeRefresh",
        isDisabled: isDisabled
      }, ___EmotionJSX("strong", null, ___EmotionJSX("small", null, autoRefeshLabel))),
      readOnly: readOnly,
      disabled: isDisabled,
      value: prettyInterval(Boolean(isPaused), refreshInterval)
    }, rest)),
    isOpen: isPopoverOpen,
    closePopover: function closePopover() {
      setIsPopoverOpen(false);
    }
  }, ___EmotionJSX(EuiRefreshInterval, {
    onRefreshChange: onRefreshChange,
    isPaused: isPaused,
    refreshInterval: refreshInterval
  }));
};
EuiAutoRefresh.propTypes = {
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
  onRefreshChange: PropTypes.func.isRequired,
  isDisabled: PropTypes.bool,

  /**
     * The input is `readOnly` by default because the input value is handled by the popover form.
     * If you need make the input `isInvalid`, you'll need to set `readOnly` to `false`.
     */
  readOnly: PropTypes.any
};
export var EuiAutoRefreshButton = function EuiAutoRefreshButton(_ref2) {
  var className = _ref2.className,
      onRefreshChange = _ref2.onRefreshChange,
      isDisabled = _ref2.isDisabled,
      _ref2$isPaused = _ref2.isPaused,
      isPaused = _ref2$isPaused === void 0 ? true : _ref2$isPaused,
      _ref2$refreshInterval = _ref2.refreshInterval,
      refreshInterval = _ref2$refreshInterval === void 0 ? 1000 : _ref2$refreshInterval,
      _ref2$shortHand = _ref2.shortHand,
      shortHand = _ref2$shortHand === void 0 ? false : _ref2$shortHand,
      _ref2$size = _ref2.size,
      size = _ref2$size === void 0 ? 's' : _ref2$size,
      _ref2$color = _ref2.color,
      color = _ref2$color === void 0 ? 'text' : _ref2$color,
      rest = _objectWithoutProperties(_ref2, ["className", "onRefreshChange", "isDisabled", "isPaused", "refreshInterval", "shortHand", "size", "color"]);

  var _useState3 = useState(false),
      _useState4 = _slicedToArray(_useState3, 2),
      isPopoverOpen = _useState4[0],
      setIsPopoverOpen = _useState4[1];

  var classes = classNames('euiAutoRefreshButton', className);
  var autoRefeshLabelOff = useEuiI18n('euiAutoRefresh.buttonLabelOff', 'Auto refresh is off');
  var autoRefeshLabelOn = useEuiI18n('euiAutoRefresh.buttonLabelOn', 'Auto refresh is on and set to {prettyInterval}', {
    prettyInterval: prettyInterval(Boolean(isPaused), refreshInterval)
  });
  return ___EmotionJSX(EuiPopover, {
    button: ___EmotionJSX(EuiButtonEmpty, _extends({
      onClick: function onClick() {
        return setIsPopoverOpen(function (isOpen) {
          return !isOpen;
        });
      },
      className: classes,
      size: size,
      color: color,
      iconType: "timeRefresh",
      title: isPaused ? autoRefeshLabelOff : autoRefeshLabelOn,
      isDisabled: isDisabled
    }, rest), prettyInterval(Boolean(isPaused), refreshInterval, shortHand)),
    isOpen: isPopoverOpen,
    closePopover: function closePopover() {
      setIsPopoverOpen(false);
    }
  }, ___EmotionJSX(EuiRefreshInterval, {
    onRefreshChange: onRefreshChange,
    isPaused: isPaused,
    refreshInterval: refreshInterval
  }));
};
EuiAutoRefreshButton.propTypes = {
  isPaused: PropTypes.bool,
  refreshInterval: PropTypes.number,
  onRefreshChange: PropTypes.func.isRequired,

  /**
     * `disabled` is also allowed
     */
  isDisabled: PropTypes.bool,

  /**
     * Reduces the time unit to a single letter
     */
  shortHand: PropTypes.bool,

  /**
     * Any of our named colors
     */
  color: PropTypes.oneOf(["primary", "danger", "text", "ghost", "success", "warning"]),
  size: PropTypes.oneOf(["xs", "s", "m"]),

  /**
     * Ensure the text of the button sits flush to the left, right, or both sides of its container
     */
  flush: PropTypes.oneOf(["left", "right", "both"]),

  /**
     * Force disables the button and changes the icon to a loading spinner
     */
  isLoading: PropTypes.bool,
  href: PropTypes.string,
  target: PropTypes.string,
  rel: PropTypes.string,
  buttonRef: PropTypes.any,

  /**
     * Object of props passed to the <span/> wrapping the button's content
     */
  contentProps: PropTypes.any,

  /**
     * Object of props passed to the <span/> wrapping the content's text/children only (not icon)
     */
  textProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,
    ref: PropTypes.any,
    "data-text": PropTypes.string
  }),
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};