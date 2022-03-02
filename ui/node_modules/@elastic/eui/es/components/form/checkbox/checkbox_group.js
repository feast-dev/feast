function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
import classNames from 'classnames';
import { EuiFormFieldset } from '../form_fieldset';
import { EuiCheckbox } from './checkbox';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiCheckboxGroup = function EuiCheckboxGroup(_ref) {
  var _ref$options = _ref.options,
      options = _ref$options === void 0 ? [] : _ref$options,
      _ref$idToSelectedMap = _ref.idToSelectedMap,
      idToSelectedMap = _ref$idToSelectedMap === void 0 ? {} : _ref$idToSelectedMap,
      onChange = _ref.onChange,
      className = _ref.className,
      disabled = _ref.disabled,
      compressed = _ref.compressed,
      legend = _ref.legend,
      rest = _objectWithoutProperties(_ref, ["options", "idToSelectedMap", "onChange", "className", "disabled", "compressed", "legend"]);

  var checkboxes = options.map(function (option, index) {
    var isOptionDisabled = option.disabled,
        optionClass = option.className,
        optionRest = _objectWithoutProperties(option, ["disabled", "className"]);

    return ___EmotionJSX(EuiCheckbox, _extends({
      className: classNames('euiCheckboxGroup__item', optionClass),
      key: index,
      checked: idToSelectedMap[option.id],
      disabled: disabled || isOptionDisabled,
      onChange: onChange.bind(null, option.id),
      compressed: compressed
    }, optionRest));
  });

  if (!!legend) {
    // Be sure to pass down the compressed option to the legend
    legend.compressed = compressed;
    return ___EmotionJSX(EuiFormFieldset, _extends({
      className: className,
      legend: legend
    }, rest), checkboxes);
  }

  return ___EmotionJSX("div", _extends({
    className: className
  }, rest), checkboxes);
};
EuiCheckboxGroup.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  options: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.string.isRequired
  }).isRequired).isRequired,
  idToSelectedMap: PropTypes.shape({}).isRequired,
  onChange: PropTypes.func.isRequired,

  /**
     * Tightens up the spacing between checkbox rows and sends down the
     * compressed prop to the checkbox itself
     */
  compressed: PropTypes.bool,
  disabled: PropTypes.bool,

  /**
     * If the individual labels for each radio do not provide a sufficient description, add a legend.
     * Wraps the group in a `EuiFormFieldset` which adds an `EuiLegend` for titling the whole group.
     * Accepts an `EuiFormLegendProps` shape.
     */
  legend: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,

    /**
         * ReactNode to render as this component's content
         */
    children: PropTypes.node.isRequired,

    /**
         * For a hidden legend that is still visible to the screen reader, set to 'hidden'
         */
    display: PropTypes.oneOf(["hidden", "visible"]),
    compressed: PropTypes.bool
  })
};