"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useI18nCurrentStep = exports.useI18nLoadingStep = exports.useI18nDisabledStep = exports.useI18nIncompleteStep = exports.useI18nErrorsStep = exports.useI18nWarningStep = exports.useI18nCompleteStep = exports.useI18nStep = void 0;

var _i18n = require("../i18n");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var useI18nStep = function useI18nStep(_ref) {
  var number = _ref.number,
      title = _ref.title;
  var string = (0, _i18n.useEuiI18n)('euiStepStrings.step', 'Step {number}: {title}', {
    number: number,
    title: title
  });
  var simpleString = (0, _i18n.useEuiI18n)('euiStepStrings.simpleStep', 'Step {number}', {
    number: number
  });
  return title ? string : simpleString;
};

exports.useI18nStep = useI18nStep;

var useI18nCompleteStep = function useI18nCompleteStep(_ref2) {
  var number = _ref2.number,
      title = _ref2.title;
  var string = (0, _i18n.useEuiI18n)('euiStepStrings.complete', 'Step {number}: {title} is complete', {
    number: number,
    title: title
  });
  var simpleString = (0, _i18n.useEuiI18n)('euiStepStrings.simpleComplete', 'Step {number} is complete', {
    number: number
  });
  return title ? string : simpleString;
};

exports.useI18nCompleteStep = useI18nCompleteStep;

var useI18nWarningStep = function useI18nWarningStep(_ref3) {
  var number = _ref3.number,
      title = _ref3.title;
  var string = (0, _i18n.useEuiI18n)('euiStepStrings.warning', 'Step {number}: {title} has warnings', {
    number: number,
    title: title
  });
  var simpleString = (0, _i18n.useEuiI18n)('euiStepStrings.simpleWarning', 'Step {number} has warnings', {
    number: number
  });
  return title ? string : simpleString;
};

exports.useI18nWarningStep = useI18nWarningStep;

var useI18nErrorsStep = function useI18nErrorsStep(_ref4) {
  var number = _ref4.number,
      title = _ref4.title;
  var string = (0, _i18n.useEuiI18n)('euiStepStrings.errors', 'Step {number}: {title} has errors', {
    number: number,
    title: title
  });
  var simpleString = (0, _i18n.useEuiI18n)('euiStepStrings.simpleErrors', 'Step {number} has errors', {
    number: number
  });
  return title ? string : simpleString;
};

exports.useI18nErrorsStep = useI18nErrorsStep;

var useI18nIncompleteStep = function useI18nIncompleteStep(_ref5) {
  var number = _ref5.number,
      title = _ref5.title;
  var string = (0, _i18n.useEuiI18n)('euiStepStrings.incomplete', 'Step {number}: {title} is incomplete', {
    number: number,
    title: title
  });
  var simpleString = (0, _i18n.useEuiI18n)('euiStepStrings.simpleIncomplete', 'Step {number} is incomplete', {
    number: number
  });
  return title ? string : simpleString;
};

exports.useI18nIncompleteStep = useI18nIncompleteStep;

var useI18nDisabledStep = function useI18nDisabledStep(_ref6) {
  var number = _ref6.number,
      title = _ref6.title;
  var string = (0, _i18n.useEuiI18n)('euiStepStrings.disabled', 'Step {number}: {title} is disabled', {
    number: number,
    title: title
  });
  var simpleString = (0, _i18n.useEuiI18n)('euiStepStrings.simpleDisabled', 'Step {number} is disabled', {
    number: number
  });
  return title ? string : simpleString;
};

exports.useI18nDisabledStep = useI18nDisabledStep;

var useI18nLoadingStep = function useI18nLoadingStep(_ref7) {
  var number = _ref7.number,
      title = _ref7.title;
  var string = (0, _i18n.useEuiI18n)('euiStepStrings.loading', 'Step {number}: {title} is loading', {
    number: number,
    title: title
  });
  var simpleString = (0, _i18n.useEuiI18n)('euiStepStrings.simpleLoading', 'Step {number} is loading', {
    number: number
  });
  return title ? string : simpleString;
};

exports.useI18nLoadingStep = useI18nLoadingStep;

var useI18nCurrentStep = function useI18nCurrentStep(_ref8) {
  var number = _ref8.number,
      title = _ref8.title;
  var string = (0, _i18n.useEuiI18n)('euiStepStrings.current', 'Current step {number}: {title}', {
    number: number,
    title: title
  });
  var simpleString = (0, _i18n.useEuiI18n)('euiStepStrings.simpleCurrent', 'Current step is {number}', {
    number: number
  });
  return title ? string : simpleString;
};

exports.useI18nCurrentStep = useI18nCurrentStep;