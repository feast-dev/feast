function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _unsupportedIterableToArray(arr) || _nonIterableSpread(); }

function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _iterableToArray(iter) { if (typeof Symbol !== "undefined" && Symbol.iterator in Object(iter)) return Array.from(iter); }

function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) return _arrayLikeToArray(arr); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

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
import classNames from 'classnames';
import chroma from 'chroma-js';
import { keysOf } from '../common';
import { euiPaletteColorBlindBehindText, getSecureRelForTarget, isColorDark } from '../../services';
import { EuiInnerText } from '../inner_text';
import { EuiIcon } from '../icon';
import { chromaValid, parseColor } from '../color_picker/utils';
import { validateHref } from '../../services/security/href_validator';
import { jsx as ___EmotionJSX } from "@emotion/react";
// TODO - replace with variables once https://github.com/elastic/eui/issues/2731 is closed
var colorInk = '#000';
var colorGhost = '#fff'; // The color blind palette has some stricter accessibility needs with regards to
// charts and contrast. We use the euiPaletteColorBlindBehindText variant here since our
// accessibility concerns pertain to foreground (text) and background contrast

var visColors = euiPaletteColorBlindBehindText();
var colorToHexMap = {
  // TODO - replace with variable once https://github.com/elastic/eui/issues/2731 is closed
  default: '#d3dae6',
  primary: visColors[1],
  success: visColors[0],
  accent: visColors[2],
  warning: visColors[5],
  danger: visColors[9]
};
export var COLORS = keysOf(colorToHexMap);
var iconSideToClassNameMap = {
  left: 'euiBadge--iconLeft',
  right: 'euiBadge--iconRight'
};
export var ICON_SIDES = keysOf(iconSideToClassNameMap);
export var EuiBadge = function EuiBadge(_ref) {
  var children = _ref.children,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'default' : _ref$color,
      iconType = _ref.iconType,
      _ref$iconSide = _ref.iconSide,
      iconSide = _ref$iconSide === void 0 ? 'left' : _ref$iconSide,
      className = _ref.className,
      _isDisabled = _ref.isDisabled,
      onClick = _ref.onClick,
      iconOnClick = _ref.iconOnClick,
      onClickAriaLabel = _ref.onClickAriaLabel,
      iconOnClickAriaLabel = _ref.iconOnClickAriaLabel,
      closeButtonProps = _ref.closeButtonProps,
      href = _ref.href,
      rel = _ref.rel,
      target = _ref.target,
      style = _ref.style,
      rest = _objectWithoutProperties(_ref, ["children", "color", "iconType", "iconSide", "className", "isDisabled", "onClick", "iconOnClick", "onClickAriaLabel", "iconOnClickAriaLabel", "closeButtonProps", "href", "rel", "target", "style"]);

  var isHrefValid = !href || validateHref(href);
  var isDisabled = _isDisabled || !isHrefValid;
  var optionalCustomStyles = style;
  var textColor = null; // TODO - replace with variable once https://github.com/elastic/eui/issues/2731 is closed

  var wcagContrastBase = 4.5; // WCAG AA contrast level

  var wcagContrast = null;
  var colorHex = null; // Check if a valid color name was provided

  try {
    if (COLORS.indexOf(color) > -1) {
      // Get the hex equivalent for the provided color name
      colorHex = colorToHexMap[color]; // Set dark or light text color based upon best contrast

      textColor = setTextColor(colorHex);
      optionalCustomStyles = _objectSpread({
        backgroundColor: colorHex,
        color: textColor
      }, optionalCustomStyles);
    } else if (color !== 'hollow') {
      // This is a custom color that is neither from the base palette nor hollow
      // Let's do our best to ensure that it provides sufficient contrast
      // Set dark or light text color based upon best contrast
      textColor = setTextColor(color); // Check the contrast

      wcagContrast = getColorContrast(textColor, color);

      if (wcagContrast < wcagContrastBase) {
        // It's low contrast, so lets show a warning in the console
        console.warn('Warning: ', color, ' badge has low contrast of ', wcagContrast.toFixed(2), '. Should be above ', wcagContrastBase, '.');
      }

      optionalCustomStyles = _objectSpread({
        backgroundColor: color,
        color: textColor
      }, optionalCustomStyles);
    }
  } catch (err) {
    handleInvalidColor(color);
  }

  var classes = classNames('euiBadge', {
    'euiBadge-isClickable': (onClick || href) && !iconOnClick,
    'euiBadge-isDisabled': isDisabled,
    'euiBadge--hollow': color === 'hollow'
  }, iconSideToClassNameMap[iconSide], className);
  var closeClassNames = classNames('euiBadge__icon', closeButtonProps && closeButtonProps.className);
  var Element = href && !isDisabled ? 'a' : 'button';
  var relObj = {};

  if (href && !isDisabled) {
    relObj.href = href;
    relObj.target = target;
    relObj.rel = getSecureRelForTarget({
      href: href,
      target: target,
      rel: rel
    });
  }

  if (onClick) {
    relObj.onClick = onClick;
  }

  var optionalIcon = null;

  if (iconType) {
    if (iconOnClick) {
      if (!iconOnClickAriaLabel) {
        console.warn('When passing the iconOnClick props to EuiBadge, you must also provide iconOnClickAriaLabel');
      }

      optionalIcon = ___EmotionJSX("button", {
        type: "button",
        className: "euiBadge__iconButton",
        "aria-label": iconOnClickAriaLabel,
        disabled: isDisabled,
        title: iconOnClickAriaLabel,
        onClick: iconOnClick
      }, ___EmotionJSX(EuiIcon, _extends({
        type: iconType,
        size: "s",
        color: "inherit" // forces the icon to inherit its parent color

      }, closeButtonProps, {
        className: closeClassNames
      })));
    } else {
      optionalIcon = ___EmotionJSX(EuiIcon, {
        type: iconType,
        size: children ? 's' : 'm',
        className: "euiBadge__icon",
        color: "inherit" // forces the icon to inherit its parent color

      });
    }
  }

  if (onClick && !onClickAriaLabel) {
    console.warn('When passing onClick to EuiBadge, you must also provide onClickAriaLabel');
  }

  var content = ___EmotionJSX("span", {
    className: "euiBadge__content"
  }, children && ___EmotionJSX("span", {
    className: "euiBadge__text"
  }, children), optionalIcon);

  if (iconOnClick) {
    return onClick || href ? ___EmotionJSX("span", {
      className: classes,
      style: optionalCustomStyles
    }, ___EmotionJSX("span", {
      className: "euiBadge__content"
    }, ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
      return ___EmotionJSX(Element, _extends({
        className: "euiBadge__childButton",
        disabled: isDisabled,
        "aria-label": onClickAriaLabel,
        ref: ref,
        title: innerText
      }, relObj, rest), children);
    }), optionalIcon)) : ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
      return ___EmotionJSX("span", _extends({
        className: classes,
        style: optionalCustomStyles,
        ref: ref,
        title: innerText
      }, rest), content);
    });
  } else if (onClick || href) {
    return ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
      return ___EmotionJSX(Element, _extends({
        disabled: isDisabled,
        "aria-label": onClickAriaLabel,
        className: classes,
        style: optionalCustomStyles,
        ref: ref,
        title: innerText
      }, relObj, rest), content);
    });
  } else {
    return ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
      return ___EmotionJSX("span", _extends({
        className: classes,
        style: optionalCustomStyles,
        ref: ref,
        title: innerText
      }, rest), content);
    });
  }
};
EuiBadge.propTypes = {
  /**
     * Accepts any string from our icon library
     */
  iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

  /**
     * The side of the badge the icon should sit
     */
  iconSide: PropTypes.oneOf(["left", "right"]),

  /**
     * Accepts either our palette colors (primary, success ..etc) or a hex value `#FFFFFF`, `#000`.
     */
  color: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.oneOf(["default", "primary", "success", "accent", "warning", "danger", "text", "subdued", "ghost", "inherit"]).isRequired]),

  /**
     * Will override any color passed through the `color` prop.
     */
  isDisabled: PropTypes.bool,

  /**
     * Props passed to the close button.
     */
  closeButtonProps: PropTypes.any,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Will apply an onclick to icon within the badge
     */
  iconOnClick: PropTypes.func,

  /**
     * Aria label applied to the iconOnClick button
     */
  iconOnClickAriaLabel: PropTypes.any,

  /**
     * Will apply an onclick to the badge itself
     */
  onClick: PropTypes.func,

  /**
     * Aria label applied to the onClick button
     */
  onClickAriaLabel: PropTypes.any,
  href: PropTypes.string,
  target: PropTypes.string,
  rel: PropTypes.string
};

function getColorContrast(textColor, color) {
  var contrastValue = chroma.contrast(textColor, color);
  return contrastValue;
}

function setTextColor(bgColor) {
  var textColor = isColorDark.apply(void 0, _toConsumableArray(chroma(bgColor).rgb())) ? colorGhost : colorInk;
  return textColor;
}

function handleInvalidColor(color) {
  var isNamedColor = color && COLORS.includes(color) || color === 'hollow';
  var isValidColorString = color && chromaValid(parseColor(color) || '');

  if (!isNamedColor && !isValidColorString) {
    console.warn('EuiBadge expects a valid color. This can either be a three or six ' + "character hex value, rgb(a) value, hsv value, hollow, or one of the following: ".concat(COLORS, ". ") + "Instead got ".concat(color, "."));
  }
}