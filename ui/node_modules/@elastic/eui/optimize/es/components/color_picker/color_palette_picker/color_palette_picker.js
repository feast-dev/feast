import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { EuiSuperSelect } from '../../form';
import { EuiColorPaletteDisplay } from '../color_palette_display';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiColorPalettePicker = function EuiColorPalettePicker(_ref) {
  var className = _ref.className,
      _ref$compressed = _ref.compressed,
      compressed = _ref$compressed === void 0 ? false : _ref$compressed,
      disabled = _ref.disabled,
      _ref$fullWidth = _ref.fullWidth,
      fullWidth = _ref$fullWidth === void 0 ? false : _ref$fullWidth,
      _ref$isInvalid = _ref.isInvalid,
      isInvalid = _ref$isInvalid === void 0 ? false : _ref$isInvalid,
      onChange = _ref.onChange,
      _ref$readOnly = _ref.readOnly,
      readOnly = _ref$readOnly === void 0 ? false : _ref$readOnly,
      valueOfSelected = _ref.valueOfSelected,
      palettes = _ref.palettes,
      append = _ref.append,
      prepend = _ref.prepend,
      _ref$selectionDisplay = _ref.selectionDisplay,
      selectionDisplay = _ref$selectionDisplay === void 0 ? 'palette' : _ref$selectionDisplay,
      rest = _objectWithoutProperties(_ref, ["className", "compressed", "disabled", "fullWidth", "isInvalid", "onChange", "readOnly", "valueOfSelected", "palettes", "append", "prepend", "selectionDisplay"]);

  var getPalette = function getPalette(_ref2) {
    var type = _ref2.type,
        palette = _ref2.palette,
        title = _ref2.title;
    return ___EmotionJSX(EuiColorPaletteDisplay, {
      type: type,
      palette: palette,
      title: title
    });
  };

  var paletteOptions = palettes.map(function (item) {
    var type = item.type,
        value = item.value,
        title = item.title,
        palette = item.palette,
        rest = _objectWithoutProperties(item, ["type", "value", "title", "palette"]);

    var paletteForDisplay = item.type !== 'text' ? getPalette(item) : null;
    return _objectSpread({
      value: String(value),
      inputDisplay: selectionDisplay === 'title' || type === 'text' ? title : paletteForDisplay,
      dropdownDisplay: ___EmotionJSX("div", {
        className: "euiColorPalettePicker__item"
      }, title && type !== 'text' && // Accessible labels are managed by color_palette_display_fixed and
      // color_palette_display_gradient. Adding the aria-hidden attribute
      // here to ensure screen readers don't speak the listbox options twice.
      ___EmotionJSX("div", {
        "aria-hidden": "true",
        className: "euiColorPalettePicker__itemTitle"
      }, title), type === 'text' ? title : paletteForDisplay)
    }, rest);
  });
  return ___EmotionJSX(EuiSuperSelect, _extends({
    className: className,
    options: paletteOptions,
    valueOfSelected: valueOfSelected,
    onChange: onChange,
    hasDividers: true,
    isInvalid: isInvalid,
    compressed: compressed,
    disabled: disabled,
    readOnly: readOnly,
    fullWidth: fullWidth,
    append: append,
    prepend: prepend
  }, rest));
};