import _extends from "@babel/runtime/helpers/extends";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";
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
import classNames from 'classnames';
import { EuiBeacon } from '../beacon';
import { EuiButtonEmpty } from '../button';
import { EuiFlexGroup, EuiFlexItem } from '../flex';
import { EuiI18n } from '../i18n';
import { EuiPopover, EuiPopoverFooter, EuiPopoverTitle } from '../popover';
import { EuiTitle } from '../title';
import { EuiTourStepIndicator } from './tour_step_indicator';
import { useGeneratedHtmlId } from '../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiTourStep = function EuiTourStep(_ref) {
  var _ref$anchorPosition = _ref.anchorPosition,
      anchorPosition = _ref$anchorPosition === void 0 ? 'leftUp' : _ref$anchorPosition,
      children = _ref.children,
      className = _ref.className,
      _ref$closePopover = _ref.closePopover,
      closePopover = _ref$closePopover === void 0 ? function () {} : _ref$closePopover,
      content = _ref.content,
      _ref$isStepOpen = _ref.isStepOpen,
      isStepOpen = _ref$isStepOpen === void 0 ? false : _ref$isStepOpen,
      _ref$minWidth = _ref.minWidth,
      minWidth = _ref$minWidth === void 0 ? 300 : _ref$minWidth,
      _ref$maxWidth = _ref.maxWidth,
      maxWidth = _ref$maxWidth === void 0 ? 600 : _ref$maxWidth,
      onFinish = _ref.onFinish,
      _ref$step = _ref.step,
      step = _ref$step === void 0 ? 1 : _ref$step,
      stepsTotal = _ref.stepsTotal,
      style = _ref.style,
      subtitle = _ref.subtitle,
      title = _ref.title,
      _ref$decoration = _ref.decoration,
      decoration = _ref$decoration === void 0 ? 'beacon' : _ref$decoration,
      footerAction = _ref.footerAction,
      rest = _objectWithoutProperties(_ref, ["anchorPosition", "children", "className", "closePopover", "content", "isStepOpen", "minWidth", "maxWidth", "onFinish", "step", "stepsTotal", "style", "subtitle", "title", "decoration", "footerAction"]);

  var titleId = useGeneratedHtmlId();

  if (step === 0) {
    console.warn('EuiTourStep `step` should 1-based indexing. Please update to eliminate 0 indexes.');
  }

  var newStyle = _objectSpread(_objectSpread({}, style), {}, {
    maxWidth: maxWidth,
    minWidth: minWidth
  });

  var classes = classNames('euiTour', className);
  var finishButtonProps = {
    color: 'text',
    flush: 'right',
    size: 'xs'
  };

  var footer = ___EmotionJSX(EuiFlexGroup, {
    responsive: false,
    justifyContent: stepsTotal > 1 ? 'spaceBetween' : 'flexEnd'
  }, stepsTotal > 1 && ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, ___EmotionJSX("ul", {
    className: "euiTourFooter__stepList"
  }, _toConsumableArray(Array(stepsTotal).keys()).map(function (_, i) {
    var status = 'complete';

    if (step === i + 1) {
      status = 'active';
    } else if (step <= i) {
      status = 'incomplete';
    }

    return ___EmotionJSX(EuiTourStepIndicator, {
      key: i,
      number: i + 1,
      status: status
    });
  }))), footerAction ? ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, footerAction) : ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, ___EmotionJSX(EuiI18n, {
    tokens: ['euiTourStep.endTour', 'euiTourStep.skipTour', 'euiTourStep.closeTour'],
    defaults: ['End tour', 'Skip tour', 'Close tour']
  }, function (_ref2) {
    var _ref3 = _slicedToArray(_ref2, 3),
        endTour = _ref3[0],
        skipTour = _ref3[1],
        closeTour = _ref3[2];

    var content = closeTour;

    if (stepsTotal > 1) {
      content = stepsTotal === step ? endTour : skipTour;
    }

    return ___EmotionJSX(EuiButtonEmpty, _extends({
      onClick: onFinish
    }, finishButtonProps), content);
  })));

  var hasBeacon = decoration === 'beacon';
  return ___EmotionJSX(EuiPopover, _extends({
    anchorPosition: anchorPosition,
    button: children,
    closePopover: closePopover,
    isOpen: isStepOpen,
    ownFocus: false,
    panelClassName: classes,
    panelStyle: newStyle,
    offset: hasBeacon ? 10 : 0,
    "aria-labelledby": titleId,
    arrowChildren: hasBeacon && ___EmotionJSX(EuiBeacon, {
      className: "euiTour__beacon"
    })
  }, rest), ___EmotionJSX(EuiPopoverTitle, {
    className: "euiTourHeader",
    id: titleId
  }, subtitle && ___EmotionJSX(EuiTitle, {
    size: "xxxs",
    className: "euiTourHeader__subtitle"
  }, ___EmotionJSX("h2", null, subtitle)), ___EmotionJSX(EuiTitle, {
    size: "xxs",
    className: "euiTourHeader__title"
  }, subtitle ? ___EmotionJSX("h3", null, title) : ___EmotionJSX("h2", null, title))), ___EmotionJSX("div", {
    className: "euiTour__content"
  }, content), ___EmotionJSX(EuiPopoverFooter, {
    className: "euiTourFooter"
  }, footer));
};