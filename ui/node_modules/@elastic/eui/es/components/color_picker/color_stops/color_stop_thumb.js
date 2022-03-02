function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useEffect, useMemo, useRef, useState } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { getPositionFromStop, getStopFromMouseLocation, isColorInvalid, isStopInvalid } from './utils';
import { getChromaColor } from '../utils';
import { keys, useMouseMove } from '../../../services';
import { EuiButtonIcon } from '../../button';
import { EuiColorPicker } from '../color_picker';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { EuiFieldNumber, EuiFormRow } from '../../form';
import { EuiI18n } from '../../i18n';
import { EuiPopover } from '../../popover';
import { EuiScreenReaderOnly } from '../../accessibility';
import { EuiSpacer } from '../../spacer';
import { EuiRangeThumb } from '../../form/range/range_thumb';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiColorStopThumb = function EuiColorStopThumb(_ref) {
  var className = _ref.className,
      stop = _ref.stop,
      color = _ref.color,
      onChange = _ref.onChange,
      onFocus = _ref.onFocus,
      onRemove = _ref.onRemove,
      globalMin = _ref.globalMin,
      globalMax = _ref.globalMax,
      localMin = _ref.localMin,
      localMax = _ref.localMax,
      min = _ref.min,
      max = _ref.max,
      _ref$isRangeMin = _ref.isRangeMin,
      isRangeMin = _ref$isRangeMin === void 0 ? false : _ref$isRangeMin,
      _ref$isRangeMax = _ref.isRangeMax,
      isRangeMax = _ref$isRangeMax === void 0 ? false : _ref$isRangeMax,
      parentRef = _ref.parentRef,
      colorPickerMode = _ref.colorPickerMode,
      colorPickerShowAlpha = _ref.colorPickerShowAlpha,
      colorPickerSwatches = _ref.colorPickerSwatches,
      disabled = _ref.disabled,
      readOnly = _ref.readOnly,
      isPopoverOpen = _ref.isPopoverOpen,
      openPopover = _ref.openPopover,
      closePopover = _ref.closePopover,
      dataIndex = _ref['data-index'],
      ariaValueText = _ref['aria-valuetext'],
      _ref$valueInputProps = _ref.valueInputProps,
      valueInputProps = _ref$valueInputProps === void 0 ? {} : _ref$valueInputProps;
  var background = useMemo(function () {
    var chromaColor = getChromaColor(color, colorPickerShowAlpha);
    return chromaColor ? chromaColor.css() : undefined;
  }, [color, colorPickerShowAlpha]);

  var _useState = useState(isPopoverOpen),
      _useState2 = _slicedToArray(_useState, 2),
      hasFocus = _useState2[0],
      setHasFocus = _useState2[1];

  var _useState3 = useState(isColorInvalid(color, colorPickerShowAlpha)),
      _useState4 = _slicedToArray(_useState3, 2),
      colorIsInvalid = _useState4[0],
      setColorIsInvalid = _useState4[1];

  var _useState5 = useState(isStopInvalid(stop)),
      _useState6 = _slicedToArray(_useState5, 2),
      stopIsInvalid = _useState6[0],
      setStopIsInvalid = _useState6[1];

  var _useState7 = useState(null),
      _useState8 = _slicedToArray(_useState7, 2),
      numberInputRef = _useState8[0],
      setNumberInputRef = _useState8[1];

  var popoverRef = useRef(null);
  useEffect(function () {
    if (isPopoverOpen && popoverRef && popoverRef.current) {
      popoverRef.current.positionPopoverFixed();
    }
  }, [isPopoverOpen, stop]);

  var getStopFromMouseLocationFn = function getStopFromMouseLocationFn(location) {
    // Guard against `null` ref in usage
    return getStopFromMouseLocation(location, parentRef, globalMin, globalMax);
  };

  var getPositionFromStopFn = function getPositionFromStopFn(stop) {
    // Guard against `null` ref in usage
    return getPositionFromStop(stop, parentRef, globalMin, globalMax);
  };

  var handleOnRemove = function handleOnRemove() {
    if (onRemove) {
      closePopover();
      onRemove();
    }
  };

  var handleFocus = function handleFocus() {
    setHasFocus(true);

    if (onFocus) {
      onFocus();
    }
  };

  var setHasFocusTrue = function setHasFocusTrue() {
    return setHasFocus(true);
  };

  var setHasFocusFalse = function setHasFocusFalse() {
    return setHasFocus(false);
  };

  var handleColorChange = function handleColorChange(value) {
    setColorIsInvalid(isColorInvalid(value, colorPickerShowAlpha));
    onChange({
      stop: stop,
      color: value
    });
  };

  var handleStopChange = function handleStopChange(value) {
    var willBeInvalid = value > localMax || value < localMin;

    if (willBeInvalid) {
      if (value > localMax) {
        value = localMax;
      }

      if (value < localMin) {
        value = localMin;
      }
    }

    setStopIsInvalid(isStopInvalid(value));
    onChange({
      stop: value,
      color: color
    });
  };

  var handleStopInputChange = function handleStopInputChange(e) {
    var value = parseFloat(e.target.value);
    var willBeInvalid = value > globalMax || value < globalMin;

    if (willBeInvalid) {
      if (value > globalMax && max != null) {
        value = globalMax;
      }

      if (value < globalMin && min != null) {
        value = globalMin;
      }
    }

    setStopIsInvalid(isStopInvalid(value));
    onChange({
      stop: value,
      color: color
    });
  };

  var handlePointerChange = function handlePointerChange(location, isFirstInteraction) {
    if (isFirstInteraction) return; // Prevents change on the initial MouseDown event

    if (parentRef == null) {
      return;
    }

    var newStop = getStopFromMouseLocationFn(location);
    handleStopChange(newStop);
  };

  var handleKeyDown = function handleKeyDown(event) {
    switch (event.key) {
      case keys.ENTER:
        event.preventDefault();
        openPopover();
        break;

      case keys.ARROW_LEFT:
        event.preventDefault();
        if (readOnly) return;
        handleStopChange(stop - 1);
        break;

      case keys.ARROW_RIGHT:
        event.preventDefault();
        if (readOnly) return;
        handleStopChange(stop + 1);
        break;
    }
  };

  var _useMouseMove = useMouseMove(handlePointerChange),
      _useMouseMove2 = _slicedToArray(_useMouseMove, 2),
      handleMouseDown = _useMouseMove2[0],
      handleInteraction = _useMouseMove2[1];

  var handleOnMouseDown = function handleOnMouseDown(e) {
    if (!readOnly) {
      handleMouseDown(e);
    }

    openPopover();
  };

  var handleTouchInteraction = function handleTouchInteraction(e) {
    if (!readOnly) {
      handleInteraction(e);
    }
  };

  var handleTouchStart = function handleTouchStart(e) {
    handleTouchInteraction(e);

    if (!isPopoverOpen) {
      openPopover();
    }
  };

  var classes = classNames('euiColorStopPopover', {
    'euiColorStopPopover-hasFocus': hasFocus || isPopoverOpen
  }, className);
  return ___EmotionJSX(EuiPopover, {
    ref: popoverRef,
    className: classes,
    anchorClassName: "euiColorStopPopover__anchor",
    panelPaddingSize: "s",
    isOpen: isPopoverOpen,
    closePopover: closePopover,
    initialFocus: numberInputRef || undefined,
    focusTrapProps: {
      clickOutsideDisables: false
    },
    panelClassName: numberInputRef ? undefined : 'euiColorStopPopover-isLoadingPanel',
    style: {
      left: "".concat(getPositionFromStopFn(stop), "%")
    },
    button: ___EmotionJSX(EuiI18n, {
      tokens: ['euiColorStopThumb.buttonAriaLabel', 'euiColorStopThumb.buttonTitle'],
      defaults: ['Press the Enter key to modify this stop. Press Escape to focus the group', 'Click to edit, drag to reposition']
    }, function (_ref2) {
      var _ref3 = _slicedToArray(_ref2, 2),
          buttonAriaLabel = _ref3[0],
          buttonTitle = _ref3[1];

      var ariaLabel = buttonAriaLabel;
      var title = buttonTitle;
      return ___EmotionJSX(EuiRangeThumb, {
        "data-test-subj": "euiColorStopThumb",
        "data-index": dataIndex,
        min: localMin,
        max: localMax,
        value: stop,
        onFocus: handleFocus,
        onBlur: setHasFocusFalse,
        onMouseOver: setHasFocusTrue,
        onMouseOut: setHasFocusFalse,
        onKeyDown: handleKeyDown,
        onMouseDown: handleOnMouseDown,
        onTouchStart: handleTouchStart,
        onTouchMove: handleTouchInteraction,
        "aria-valuetext": ariaValueText,
        "aria-label": ariaLabel,
        title: title,
        className: "euiColorStopThumb",
        tabIndex: -1,
        style: {
          background: background
        },
        disabled: disabled
      });
    })
  }, ___EmotionJSX("div", {
    className: "euiColorStop",
    "data-test-subj": "euiColorStopPopover"
  }, ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", {
    "aria-live": "polite"
  }, ___EmotionJSX(EuiI18n, {
    token: "euiColorStopThumb.screenReaderAnnouncement",
    default: "A popup with a color stop edit form opened. Tab forward to cycle through form controls or press escape to close this popup."
  }))), ___EmotionJSX(EuiFlexGroup, {
    gutterSize: "s",
    responsive: false
  }, ___EmotionJSX(EuiFlexItem, null, ___EmotionJSX(EuiI18n, {
    tokens: ['euiColorStopThumb.stopLabel', 'euiColorStopThumb.stopErrorMessage'],
    defaults: ['Stop value', 'Value is out of range']
  }, function (_ref4) {
    var _ref5 = _slicedToArray(_ref4, 2),
        stopLabel = _ref5[0],
        stopErrorMessage = _ref5[1];

    return ___EmotionJSX(EuiFormRow, {
      label: stopLabel,
      display: "rowCompressed",
      isInvalid: stopIsInvalid,
      error: stopIsInvalid ? stopErrorMessage : null
    }, ___EmotionJSX(EuiFieldNumber, _extends({}, valueInputProps, {
      inputRef: setNumberInputRef,
      compressed: true,
      readOnly: readOnly,
      min: isRangeMin || min == null ? undefined : localMin,
      max: isRangeMax || max == null ? undefined : localMax,
      value: isStopInvalid(stop) ? '' : stop,
      isInvalid: stopIsInvalid,
      onChange: handleStopInputChange
    })));
  })), !readOnly && ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, ___EmotionJSX(EuiFormRow, {
    display: "rowCompressed",
    hasEmptyLabelSpace: true
  }, ___EmotionJSX(EuiI18n, {
    token: "euiColorStopThumb.removeLabel",
    default: "Remove this stop"
  }, function (removeLabel) {
    return ___EmotionJSX(EuiButtonIcon, {
      iconType: "trash",
      color: "danger",
      "aria-label": removeLabel,
      title: removeLabel,
      disabled: !onRemove,
      onClick: handleOnRemove
    });
  })))), !readOnly && ___EmotionJSX(EuiSpacer, {
    size: "s"
  }), ___EmotionJSX(EuiColorPicker, {
    readOnly: readOnly,
    onChange: handleColorChange,
    color: color,
    mode: readOnly ? 'secondaryInput' : colorPickerMode,
    swatches: colorPickerSwatches,
    display: "inline",
    showAlpha: colorPickerShowAlpha,
    isInvalid: colorIsInvalid,
    secondaryInputDisplay: colorPickerMode === 'swatch' ? 'none' : 'bottom'
  })));
};
EuiColorStopThumb.propTypes = {
  className: PropTypes.string,
  onChange: PropTypes.func.isRequired,
  onFocus: PropTypes.func,
  onRemove: PropTypes.func,
  globalMin: PropTypes.number.isRequired,
  globalMax: PropTypes.number.isRequired,
  localMin: PropTypes.number.isRequired,
  localMax: PropTypes.number.isRequired,
  min: PropTypes.number,
  max: PropTypes.number,
  isRangeMin: PropTypes.bool,
  isRangeMax: PropTypes.bool,
  parentRef: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.oneOf([null])]),
  colorPickerMode: PropTypes.oneOf(["default", "swatch", "picker", "secondaryInput"]).isRequired,
  colorPickerShowAlpha: PropTypes.bool,
  colorPickerSwatches: PropTypes.arrayOf(PropTypes.string.isRequired),
  disabled: PropTypes.bool,
  readOnly: PropTypes.bool,
  isPopoverOpen: PropTypes.bool.isRequired,
  openPopover: PropTypes.func.isRequired,
  closePopover: PropTypes.func.isRequired,
  "data-index": PropTypes.string,
  "aria-valuetext": PropTypes.string,
  valueInputProps: PropTypes.any,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  stop: PropTypes.number.isRequired,
  color: PropTypes.string.isRequired
};