function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import PropTypes from "prop-types";
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
EuiColorPalettePicker.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  compressed: PropTypes.bool,
  fullWidth: PropTypes.bool,
  isInvalid: PropTypes.bool,
  isLoading: PropTypes.bool,
  readOnly: PropTypes.bool,
  name: PropTypes.string,

  /**
     * Creates an input group with element(s) coming before input.
     * `string` | `ReactElement` or an array of these
     */
  prepend: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),

  /**
     * Creates an input group with element(s) coming after input.
     * `string` | `ReactElement` or an array of these
     */
  append: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),

  /**
     * Creates a semantic label ID for the `div[role="listbox"]` that's opened on click or keypress.
     * __Generated and passed down by `EuiSuperSelect`.__
     */
  screenReaderId: PropTypes.string,
  valueOfSelected: PropTypes.any,

  /**
       * Classes for the context menu item
       */
  itemClassName: PropTypes.string,

  /**
       * You must pass an `onChange` function to handle the update of the value
       */
  onChange: PropTypes.func,
  onFocus: PropTypes.func,
  onBlur: PropTypes.func,

  /**
       * Controls whether the options are shown. Default: false
       */
  isOpen: PropTypes.bool,

  /**
       * Optional props to pass to the underlying [EuiPopover](/#/layout/popover).
       * Allows fine-grained control of the popover dropdown menu, including
       * `repositionOnScroll` for EuiSuperSelects used within scrollable containers,
       * and customizing popover panel styling.
       *
       * Does not accept a nested `popoverProps.isOpen` property - use the top level
       * `isOpen` API instead.
       */
  popoverProps: PropTypes.any,

  /**
       *  Specify what should be displayed after a selection: a `palette` or `title`
       */
  selectionDisplay: PropTypes.oneOf(["palette", "title"]),

  /**
       * An array of one of the following objects: #EuiColorPalettePickerPaletteText, #EuiColorPalettePickerPaletteFixed, #EuiColorPalettePickerPaletteGradient
       */
  palettes: PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.shape({
    /**
       *  For storing unique value of item
       */
    value: PropTypes.string.isRequired,

    /**
       *  The name of your palette
       */
    title: PropTypes.string.isRequired,

    /**
       * `text`: a text only option (a title is required).
       */
    type: PropTypes.oneOf(["text"]).isRequired,

    /**
       * Array of color `strings` or an array of #ColorStop. The stops must be numbers in an ordered range.
       */
    palette: PropTypes.oneOfType([PropTypes.arrayOf(PropTypes.string.isRequired).isRequired, PropTypes.arrayOf(PropTypes.shape({
      stop: PropTypes.number.isRequired,
      color: PropTypes.string.isRequired
    }).isRequired).isRequired]),
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired, PropTypes.shape({
    /**
       *  For storing unique value of item
       */
    value: PropTypes.string.isRequired,

    /**
       *  The name of your palette
       */
    title: PropTypes.string,

    /**
       * `fixed`: individual color blocks
       */
    type: PropTypes.oneOf(["fixed"]).isRequired,

    /**
       * Array of color `strings` or an array of #ColorStop. The stops must be numbers in an ordered range.
       */
    palette: PropTypes.oneOfType([PropTypes.arrayOf(PropTypes.string.isRequired).isRequired, PropTypes.arrayOf(PropTypes.shape({
      stop: PropTypes.number.isRequired,
      color: PropTypes.string.isRequired
    }).isRequired).isRequired]).isRequired,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired, PropTypes.shape({
    /**
       *  For storing unique value of item
       */
    value: PropTypes.string.isRequired,

    /**
       *  The name of your palette
       */
    title: PropTypes.string,

    /**
       * `gradient`: each color fades into the next
       */
    type: PropTypes.oneOf(["gradient"]).isRequired,

    /**
       * Array of color `strings` or an array of #ColorStop. The stops must be numbers in an ordered range.
       */
    palette: PropTypes.oneOfType([PropTypes.arrayOf(PropTypes.string.isRequired).isRequired, PropTypes.arrayOf(PropTypes.shape({
      stop: PropTypes.number.isRequired,
      color: PropTypes.string.isRequired
    }).isRequired).isRequired]).isRequired,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired]).isRequired).isRequired
};