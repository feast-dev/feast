import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _typeof from "@babel/runtime/helpers/typeof";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { cloneElement, useEffect, useMemo, useRef, useState } from 'react';
import classNames from 'classnames';
import chroma from 'chroma-js';
import { EuiFieldText, EuiFormControlLayout, EuiFormRow, EuiRange } from '../form';
import { useEuiI18n } from '../i18n';
import { EuiPopover } from '../popover';
import { EuiSpacer } from '../spacer';
import { VISUALIZATION_COLORS, keys } from '../../services';
import { EuiColorPickerSwatch } from './color_picker_swatch';
import { EuiHue } from './hue';
import { EuiSaturation } from './saturation';
import { getChromaColor, parseColor, HEX_FALLBACK, HSV_FALLBACK, RGB_FALLBACK, RGB_JOIN } from './utils';
import { jsx as ___EmotionJSX } from "@emotion/react";

function isKeyboardEvent(event) {
  return _typeof(event) === 'object' && 'key' in event;
}

var getOutput = function getOutput(text) {
  var showAlpha = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
  var color = getChromaColor(text, true);
  var isValid = true;

  if (!showAlpha && color !== null) {
    isValid = color.alpha() === 1;
  } // Note that if a consumer has disallowed opacity,
  // we still return the color with an alpha channel, but mark it as invalid


  return color ? {
    rgba: color.rgba(),
    hex: color.hex(),
    isValid: isValid
  } : {
    rgba: RGB_FALLBACK,
    hex: HEX_FALLBACK,
    isValid: false
  };
};

var getHsv = function getHsv(hsv) {
  var fallback = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 0;
  // Chroma's passthrough (RGB) parsing determines that black/white/gray are hue-less and returns `NaN`
  // For our purposes we can process `NaN` as `0` if necessary
  if (!hsv) return HSV_FALLBACK;
  var hue = isNaN(hsv[0]) ? fallback : hsv[0];
  return [hue, hsv[1], hsv[2]];
};

export var EuiColorPicker = function EuiColorPicker(_ref) {
  var button = _ref.button,
      className = _ref.className,
      color = _ref.color,
      _ref$compressed = _ref.compressed,
      compressed = _ref$compressed === void 0 ? false : _ref$compressed,
      disabled = _ref.disabled,
      _ref$display = _ref.display,
      display = _ref$display === void 0 ? 'default' : _ref$display,
      _ref$fullWidth = _ref.fullWidth,
      fullWidth = _ref$fullWidth === void 0 ? false : _ref$fullWidth,
      id = _ref.id,
      isInvalid = _ref.isInvalid,
      _ref$mode = _ref.mode,
      mode = _ref$mode === void 0 ? 'default' : _ref$mode,
      onBlur = _ref.onBlur,
      onChange = _ref.onChange,
      onFocus = _ref.onFocus,
      _ref$readOnly = _ref.readOnly,
      readOnly = _ref$readOnly === void 0 ? false : _ref$readOnly,
      _ref$swatches = _ref.swatches,
      swatches = _ref$swatches === void 0 ? VISUALIZATION_COLORS : _ref$swatches,
      popoverZIndex = _ref.popoverZIndex,
      prepend = _ref.prepend,
      append = _ref.append,
      _ref$showAlpha = _ref.showAlpha,
      showAlpha = _ref$showAlpha === void 0 ? false : _ref$showAlpha,
      format = _ref.format,
      _ref$secondaryInputDi = _ref.secondaryInputDisplay,
      secondaryInputDisplay = _ref$secondaryInputDi === void 0 ? 'none' : _ref$secondaryInputDi,
      _ref$isClearable = _ref.isClearable,
      isClearable = _ref$isClearable === void 0 ? false : _ref$isClearable,
      placeholder = _ref.placeholder,
      dataTestSubj = _ref['data-test-subj'];

  var _useEuiI18n = useEuiI18n(['euiColorPicker.popoverLabel', 'euiColorPicker.colorLabel', 'euiColorPicker.colorErrorMessage', 'euiColorPicker.transparent', 'euiColorPicker.alphaLabel', 'euiColorPicker.openLabel', 'euiColorPicker.closeLabel'], ['Color selection dialog', 'Color value', 'Invalid color value', 'Transparent', 'Alpha channel (opacity) value', 'Press the escape key to close the popover', 'Press the down key to open a popover containing color options']),
      _useEuiI18n2 = _slicedToArray(_useEuiI18n, 7),
      popoverLabel = _useEuiI18n2[0],
      colorLabel = _useEuiI18n2[1],
      colorErrorMessage = _useEuiI18n2[2],
      transparent = _useEuiI18n2[3],
      alphaLabel = _useEuiI18n2[4],
      openLabel = _useEuiI18n2[5],
      closeLabel = _useEuiI18n2[6];

  var preferredFormat = useMemo(function () {
    if (format) return format;
    var parsed = parseColor(color);
    return parsed != null && _typeof(parsed) === 'object' ? 'rgba' : 'hex';
  }, [color, format]);
  var chromaColor = useMemo(function () {
    return getChromaColor(color, showAlpha);
  }, [color, showAlpha]);

  var _useState = useState('100'),
      _useState2 = _slicedToArray(_useState, 2),
      alphaRangeValue = _useState2[0],
      setAlphaRangeValue = _useState2[1];

  var alphaChannel = useMemo(function () {
    return chromaColor ? chromaColor.alpha() : 1;
  }, [chromaColor]);
  useEffect(function () {
    var percent = (alphaChannel * 100).toFixed();
    setAlphaRangeValue(percent);
  }, [alphaChannel]);

  var _useState3 = useState(false),
      _useState4 = _slicedToArray(_useState3, 2),
      isColorSelectorShown = _useState4[0],
      setIsColorSelectorShown = _useState4[1];

  var _useState5 = useState(null),
      _useState6 = _slicedToArray(_useState5, 2),
      inputRef = _useState6[0],
      setInputRef = _useState6[1]; // Ideally this is uses `useRef`, but `EuiFieldText` isn't ready for that


  var prevColor = useRef(chromaColor ? chromaColor.rgba().join() : null);

  var _useState7 = useState(chromaColor ? getHsv(chromaColor.hsv()) : HSV_FALLBACK),
      _useState8 = _slicedToArray(_useState7, 2),
      colorAsHsv = _useState8[0],
      setColorAsHsv = _useState8[1];

  var usableHsv = useMemo(function () {
    if (chromaColor && chromaColor.rgba().join() !== prevColor.current) {
      var _chromaColor$hsv = chromaColor.hsv(),
          _chromaColor$hsv2 = _slicedToArray(_chromaColor$hsv, 3),
          h = _chromaColor$hsv2[0],
          s = _chromaColor$hsv2[1],
          v = _chromaColor$hsv2[2];

      var hue = isNaN(h) ? colorAsHsv[0] : h;
      return [hue, s, v];
    }

    return colorAsHsv;
  }, [chromaColor, colorAsHsv]);
  var saturationRef = useRef(null);
  var swatchRef = useRef(null);
  var testSubjAnchor = classNames('euiColorPickerAnchor', dataTestSubj);

  var updateColorAsHsv = function updateColorAsHsv(_ref2) {
    var _ref3 = _slicedToArray(_ref2, 3),
        h = _ref3[0],
        s = _ref3[1],
        v = _ref3[2];

    setColorAsHsv(getHsv([h, s, v], usableHsv[0]));
  };

  var classes = classNames('euiColorPicker', className);
  var popoverClass = 'euiColorPicker__popoverAnchor';
  var panelClasses = classNames('euiColorPicker__popoverPanel', {
    'euiColorPicker__popoverPanel--pickerOnly': mode === 'picker' && secondaryInputDisplay !== 'bottom',
    'euiColorPicker__popoverPanel--customButton': button
  });
  var swatchClass = 'euiColorPicker__swatchSelect';
  var inputClasses = classNames('euiColorPicker__input', {
    'euiColorPicker__input--inGroup': prepend || append
  });

  var handleOnChange = function handleOnChange(text) {
    var output = getOutput(text, showAlpha);

    if (output.isValid) {
      prevColor.current = output.rgba.join();
    }

    onChange(text, output);
  };

  var handleOnBlur = function handleOnBlur() {
    // `onBlur` also gets called when the popover is closing
    // so prevent a second `onBlur` if the popover is open
    if (!isColorSelectorShown && onBlur) {
      onBlur();
    }
  };

  var closeColorSelector = function closeColorSelector() {
    var shouldDelay = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;

    if (onBlur) {
      onBlur();
    }

    if (shouldDelay) {
      setTimeout(function () {
        return setIsColorSelectorShown(false);
      });
    } else {
      setIsColorSelectorShown(false);
    }
  };

  var showColorSelector = function showColorSelector() {
    if (isColorSelectorShown || readOnly) return;

    if (onFocus) {
      onFocus();
    }

    setIsColorSelectorShown(true);
  };

  var handleToggle = function handleToggle() {
    if (isColorSelectorShown) {
      closeColorSelector();
    } else {
      showColorSelector();
    }
  };

  var handleFinalSelection = function handleFinalSelection() {
    // When the trigger is an input, focus the input so you can adjust
    if (inputRef) {
      inputRef.focus();
    }

    closeColorSelector(true);
  };

  var handleOnKeyDown = function handleOnKeyDown(event) {
    if (event.key === keys.ENTER) {
      if (isColorSelectorShown) {
        handleFinalSelection();
      } else {
        showColorSelector();
      }
    }
  };

  var handleInputActivity = function handleInputActivity(event) {
    if (isKeyboardEvent(event)) {
      if (event.key === keys.ENTER) {
        event.preventDefault();
        handleToggle();
      }
    } else {
      showColorSelector();
    }
  };

  var handleToggleOnKeyDown = function handleToggleOnKeyDown(event) {
    if (event.key === keys.ARROW_DOWN) {
      event.preventDefault();

      if (isColorSelectorShown) {
        var nextFocusEl = mode !== 'swatch' ? saturationRef : swatchRef;

        if (nextFocusEl.current) {
          nextFocusEl.current.focus();
        }
      } else {
        showColorSelector();
      }
    }
  };

  var handleColorInput = function handleColorInput(event) {
    handleOnChange(event.target.value);
    var newColor = getChromaColor(event.target.value, showAlpha);

    if (newColor) {
      updateColorAsHsv(newColor.hsv());
    }
  };

  var handleClearInput = function handleClearInput() {
    handleOnChange('');

    if (secondaryInputDisplay === 'none' && isColorSelectorShown) {
      closeColorSelector();
    }
  };

  var updateWithHsv = function updateWithHsv(hsv) {
    var color = chroma.hsv.apply(chroma, _toConsumableArray(hsv)).alpha(alphaChannel);
    var formatted;

    if (preferredFormat === 'rgba') {
      formatted = alphaChannel < 1 ? color.rgba().join(RGB_JOIN) : color.rgb().join(RGB_JOIN);
    } else {
      formatted = color.hex();
    }

    handleOnChange(formatted);
    updateColorAsHsv(hsv);
  };

  var handleColorSelection = function handleColorSelection(color) {
    var _usableHsv = _slicedToArray(usableHsv, 1),
        h = _usableHsv[0];

    var _color = _slicedToArray(color, 3),
        s = _color[1],
        v = _color[2];

    var newHsv = [h, s, v];
    updateWithHsv(newHsv);
  };

  var handleHueSelection = function handleHueSelection(hue) {
    var _usableHsv2 = _slicedToArray(usableHsv, 3),
        s = _usableHsv2[1],
        v = _usableHsv2[2];

    var newHsv = [hue, s, v];
    updateWithHsv(newHsv);
  };

  var handleSwatchSelection = function handleSwatchSelection(color) {
    var newColor = getChromaColor(color, showAlpha);
    handleOnChange(color);

    if (newColor) {
      updateColorAsHsv(newColor.hsv());
    }

    handleFinalSelection();
  };

  var handleAlphaSelection = function handleAlphaSelection(e, isValid) {
    var target = e.target;
    setAlphaRangeValue(target.value || '');

    if (isValid) {
      var alpha = parseInt(target.value, 10) / 100;
      var newColor = chromaColor ? chromaColor.alpha(alpha) : null;
      var hex = newColor ? newColor.hex() : HEX_FALLBACK;
      var rgba = newColor ? newColor.rgba() : RGB_FALLBACK;

      var _text;

      if (preferredFormat === 'rgba') {
        _text = alpha < 1 ? rgba.join(RGB_JOIN) : rgba.slice(0, 3).join(RGB_JOIN);
      } else {
        _text = hex;
      }

      onChange(_text, {
        hex: hex,
        rgba: rgba,
        isValid: !!newColor
      });
    }
  };

  var inlineInput = secondaryInputDisplay !== 'none' && ___EmotionJSX(EuiFormRow, {
    display: "rowCompressed",
    isInvalid: isInvalid,
    error: isInvalid ? colorErrorMessage : null
  }, ___EmotionJSX(EuiFormControlLayout, {
    clear: isClearable && color && !readOnly && !disabled ? {
      onClick: handleClearInput
    } : undefined,
    readOnly: readOnly,
    compressed: compressed
  }, ___EmotionJSX(EuiFieldText, {
    compressed: true,
    value: color ? color.toUpperCase() : HEX_FALLBACK,
    placeholder: !color ? placeholder || transparent : undefined,
    onChange: handleColorInput,
    isInvalid: isInvalid,
    disabled: disabled,
    readOnly: readOnly,
    "aria-label": colorLabel,
    autoComplete: "off",
    "data-test-subj": "euiColorPickerInput_".concat(secondaryInputDisplay)
  })));

  var showSecondaryInputOnly = mode === 'secondaryInput';
  var showPicker = mode !== 'swatch' && !showSecondaryInputOnly;
  var showSwatches = mode !== 'picker' && !showSecondaryInputOnly;

  var composite = ___EmotionJSX(React.Fragment, null, secondaryInputDisplay === 'top' && ___EmotionJSX(React.Fragment, null, inlineInput, ___EmotionJSX(EuiSpacer, {
    size: "s"
  })), showPicker && ___EmotionJSX("div", {
    onKeyDown: handleOnKeyDown
  }, ___EmotionJSX(EuiSaturation, {
    id: id,
    color: usableHsv,
    hex: chromaColor ? chromaColor.hex() : undefined,
    onChange: handleColorSelection,
    ref: saturationRef
  }), ___EmotionJSX(EuiHue, {
    id: id,
    hue: usableHsv[0],
    hex: chromaColor ? chromaColor.hex() : undefined,
    onChange: handleHueSelection
  })), showSwatches && ___EmotionJSX("ul", {
    className: "euiColorPicker__swatches"
  }, swatches.map(function (swatch, index) {
    return ___EmotionJSX("li", {
      className: "euiColorPicker__swatch-item",
      key: swatch
    }, ___EmotionJSX(EuiColorPickerSwatch, {
      className: swatchClass,
      color: swatch,
      onClick: function onClick() {
        return handleSwatchSelection(swatch);
      },
      ref: index === 0 ? swatchRef : undefined
    }));
  })), secondaryInputDisplay === 'bottom' && ___EmotionJSX(React.Fragment, null, mode !== 'picker' && ___EmotionJSX(EuiSpacer, {
    size: "s"
  }), inlineInput), showAlpha && ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiSpacer, {
    size: "s"
  }), ___EmotionJSX(EuiRange, {
    className: "euiColorPicker__alphaRange",
    "data-test-subj": "euiColorPickerAlpha",
    compressed: true,
    showInput: true,
    max: 100,
    min: 0,
    value: alphaRangeValue,
    append: "%",
    onChange: handleAlphaSelection,
    "aria-label": alphaLabel
  })));

  var buttonOrInput;

  if (button) {
    buttonOrInput = /*#__PURE__*/cloneElement(button, {
      onClick: handleToggle,
      id: id,
      disabled: disabled,
      'data-test-subj': testSubjAnchor
    });
  } else {
    var colorStyle = chromaColor ? chromaColor.css() : undefined;
    buttonOrInput = ___EmotionJSX(EuiFormControlLayout, {
      icon: !readOnly ? {
        type: 'arrowDown',
        side: 'right'
      } : undefined,
      clear: isClearable && color && !readOnly && !disabled ? {
        onClick: handleClearInput
      } : undefined,
      readOnly: readOnly,
      fullWidth: fullWidth,
      compressed: compressed,
      onKeyDown: handleToggleOnKeyDown,
      prepend: prepend,
      append: append
    }, ___EmotionJSX("div", {
      // Used to pass the chosen color through to form layout SVG using currentColor
      style: {
        color: colorStyle
      }
    }, ___EmotionJSX(EuiFieldText, {
      className: inputClasses,
      onClick: handleInputActivity,
      onKeyDown: handleInputActivity,
      onBlur: handleOnBlur,
      value: color ? color.toUpperCase() : HEX_FALLBACK,
      placeholder: !color ? placeholder || transparent : undefined,
      id: id,
      onChange: handleColorInput,
      icon: chromaColor ? 'swatchInput' : 'stopSlash',
      inputRef: setInputRef,
      isInvalid: isInvalid,
      compressed: compressed,
      disabled: disabled,
      readOnly: readOnly,
      fullWidth: fullWidth,
      autoComplete: "off",
      "data-test-subj": testSubjAnchor,
      "aria-label": isColorSelectorShown ? openLabel : closeLabel
    })));
  }

  return display === 'inline' ? ___EmotionJSX("div", {
    className: classes
  }, composite) : ___EmotionJSX(EuiPopover, {
    initialFocus: inputRef !== null && inputRef !== void 0 ? inputRef : undefined,
    button: buttonOrInput,
    isOpen: isColorSelectorShown,
    closePopover: handleFinalSelection,
    zIndex: popoverZIndex,
    className: popoverClass,
    panelClassName: panelClasses,
    display: button ? 'inlineBlock' : 'block',
    attachToAnchor: button ? false : true,
    anchorPosition: "downLeft",
    panelPaddingSize: "s",
    tabIndex: -1,
    "aria-label": popoverLabel,
    focusTrapProps: inputRef ? {
      shards: [inputRef]
    } : undefined
  }, ___EmotionJSX("div", {
    className: classes,
    "data-test-subj": "euiColorPickerPopover"
  }, composite));
};