function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _unsupportedIterableToArray(arr) || _nonIterableSpread(); }

function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _iterableToArray(iter) { if (typeof Symbol !== "undefined" && Symbol.iterator in Object(iter)) return Array.from(iter); }

function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) return _arrayLikeToArray(arr); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

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
import defaults from 'lodash/defaults';
import classNames from 'classnames';
import { keysOf } from '../common';
import { isColorDark, hexToRgb } from '../../services';
import { EuiIcon } from '../icon';
import { TOKEN_MAP } from './token_map';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassMap = {
  xs: 'euiToken--xsmall',
  s: 'euiToken--small',
  m: 'euiToken--medium',
  l: 'euiToken--large'
};
export var SIZES = keysOf(sizeToClassMap);
var shapeToClassMap = {
  circle: 'euiToken--circle',
  square: 'euiToken--square',
  rectangle: 'euiToken--rectangle'
};
export var SHAPES = keysOf(shapeToClassMap);
var fillToClassMap = {
  none: null,
  light: 'euiToken--light',
  dark: 'euiToken--dark'
};
export var FILLS = keysOf(fillToClassMap);
var colorToClassMap = {
  euiColorVis0: 'euiToken--euiColorVis0',
  euiColorVis1: 'euiToken--euiColorVis1',
  euiColorVis2: 'euiToken--euiColorVis2',
  euiColorVis3: 'euiToken--euiColorVis3',
  euiColorVis4: 'euiToken--euiColorVis4',
  euiColorVis5: 'euiToken--euiColorVis5',
  euiColorVis6: 'euiToken--euiColorVis6',
  euiColorVis7: 'euiToken--euiColorVis7',
  euiColorVis8: 'euiToken--euiColorVis8',
  euiColorVis9: 'euiToken--euiColorVis9',
  gray: 'euiToken--gray'
};
export var COLORS = keysOf(colorToClassMap);
export var EuiToken = function EuiToken(_ref) {
  var iconType = _ref.iconType,
      color = _ref.color,
      fill = _ref.fill,
      shape = _ref.shape,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 's' : _ref$size,
      _ref$style = _ref.style,
      style = _ref$style === void 0 ? {} : _ref$style,
      className = _ref.className,
      title = _ref.title,
      ariaLabel = _ref['aria-label'],
      ariaLabelledby = _ref['aria-labelledby'],
      ariaDescribedby = _ref['aria-describedby'],
      rest = _objectWithoutProperties(_ref, ["iconType", "color", "fill", "shape", "size", "style", "className", "title", "aria-label", "aria-labelledby", "aria-describedby"]);

  // Set the icon size to the same as the passed size
  // unless they passed `xs` which IconSize doesn't support
  var finalSize = size === 'xs' ? 's' : size; // When displaying at the small size, the token specific icons
  // should actually be displayed at medium size

  if (typeof iconType === 'string' && iconType.indexOf('token') === 0 && size === 's') {
    finalSize = 'm';
  }

  var currentDisplay = {
    color: color,
    fill: fill,
    shape: shape
  };
  var finalDisplay; // If the iconType passed is one of the prefab token types,
  // grab its properties

  if (typeof iconType === 'string' && iconType in TOKEN_MAP) {
    var tokenDisplay = TOKEN_MAP[iconType];
    finalDisplay = defaults(currentDisplay, tokenDisplay);
  } else {
    finalDisplay = currentDisplay;
  }

  var finalColor = finalDisplay.color || 'gray';
  var finalShape = finalDisplay.shape || 'circle';
  var finalFill = finalDisplay.fill || 'light'; // Color can be a named space via euiColorVis

  var colorClass;

  if (finalColor in colorToClassMap) {
    colorClass = colorToClassMap[finalColor];
  } // Or it can be a string which adds inline styles for the
  else {
      // text color if fill='none' or
      if (finalFill === 'none') {
        style.color = finalColor;
      } // full background color if fill='dark' and overrides fill='light' with dark
      else {
          finalFill = 'dark';
          style.backgroundColor = finalColor;
          style.color = isColorDark.apply(void 0, _toConsumableArray(hexToRgb(finalColor))) ? '#FFFFFF' : '#000000';
        }
    }

  var classes = classNames('euiToken', colorClass, shapeToClassMap[finalShape], fillToClassMap[finalFill], sizeToClassMap[size], className);
  return ___EmotionJSX("span", _extends({
    className: classes,
    style: style
  }, rest), ___EmotionJSX(EuiIcon, {
    type: iconType,
    size: finalSize,
    title: title,
    "aria-label": ariaLabel,
    "aria-labelledby": ariaLabelledby,
    "aria-describedby": ariaDescribedby
  }));
};
EuiToken.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * An EUI icon type
     */
  iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]).isRequired,

  /**
     * For best results use one of the vis color names (or 'gray').
     * Or supply your own color (can be used with dark or no fill only).
     * Default: `gray`
     */
  color: PropTypes.oneOfType([PropTypes.oneOf(["euiColorVis0", "euiColorVis1", "euiColorVis2", "euiColorVis3", "euiColorVis4", "euiColorVis5", "euiColorVis6", "euiColorVis7", "euiColorVis8", "euiColorVis9", "gray"]).isRequired, PropTypes.string.isRequired]),

  /**
     * Outer shape surrounding the icon
     * Default: `circle`
     */
  shape: PropTypes.oneOf(["circle", "square", "rectangle"]),

  /**
     * `light` for lightened color with border, `dark` for solid, or `none`
     * Default: `light`
     */
  fill: PropTypes.oneOf(["dark", "light", "none"]),

  /**
     * Size of the token
     */
  size: PropTypes.oneOf(["xs", "s", "m", "l"]),

  /**
     * The icon's title. Required for accessibility
     */
  title: PropTypes.string,
  "aria-labelledby": PropTypes.string,
  "aria-describedby": PropTypes.string
};