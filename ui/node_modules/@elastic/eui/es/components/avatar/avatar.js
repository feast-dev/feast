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
import { keysOf } from '../common';
import classNames from 'classnames';
import { isColorDark, hexToRgb, isValidHex } from '../../services/color';
import { euiPaletteColorBlindBehindText, toInitials } from '../../services';
import { EuiIcon } from '../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  s: 'euiAvatar--s',
  m: 'euiAvatar--m',
  l: 'euiAvatar--l',
  xl: 'euiAvatar--xl'
};
export var SIZES = keysOf(sizeToClassNameMap);
var typeToClassNameMap = {
  space: 'euiAvatar--space',
  user: 'euiAvatar--user'
};
export var TYPES = keysOf(typeToClassNameMap);
export var EuiAvatar = function EuiAvatar(_ref) {
  var className = _ref.className,
      color = _ref.color,
      imageUrl = _ref.imageUrl,
      initials = _ref.initials,
      initialsLength = _ref.initialsLength,
      iconType = _ref.iconType,
      iconSize = _ref.iconSize,
      iconColor = _ref.iconColor,
      name = _ref.name,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'user' : _ref$type,
      _ref$isDisabled = _ref.isDisabled,
      isDisabled = _ref$isDisabled === void 0 ? false : _ref$isDisabled,
      style = _ref.style,
      rest = _objectWithoutProperties(_ref, ["className", "color", "imageUrl", "initials", "initialsLength", "iconType", "iconSize", "iconColor", "name", "size", "type", "isDisabled", "style"]);

  var visColors = euiPaletteColorBlindBehindText();
  var classes = classNames('euiAvatar', sizeToClassNameMap[size], typeToClassNameMap[type], {
    'euiAvatar-isDisabled': isDisabled,
    'euiAvatar--plain': color === 'plain'
  }, className);
  checkValidInitials(initials);
  var avatarStyle = style || {};
  var iconCustomColor = iconColor;
  var isNamedColor = color === 'plain' || color === null;

  if (!isNamedColor) {
    checkValidColor(color);
    var assignedColor = color || visColors[Math.floor(name.length % visColors.length)];
    var textColor = isColorDark.apply(void 0, _toConsumableArray(hexToRgb(assignedColor))) ? '#FFFFFF' : '#000000';
    avatarStyle.backgroundColor = assignedColor;
    avatarStyle.color = textColor; // Allow consumers to let the icons keep their default color (like app icons)
    // when passing `iconColor = null`, otherwise continue to pass on `iconColor` or adjust with textColor

    iconCustomColor = iconColor || iconColor === null ? iconColor : textColor;
  }

  if (imageUrl) {
    avatarStyle.backgroundImage = "url(".concat(imageUrl, ")");
  }

  var content;

  if (!imageUrl && !iconType) {
    // Create the initials
    var calculatedInitials = toInitials(name, initialsLength, initials);
    content = ___EmotionJSX("span", {
      "aria-hidden": "true"
    }, calculatedInitials);
  } else if (iconType) {
    content = ___EmotionJSX(EuiIcon, {
      className: "euiAvatar__icon",
      size: iconSize || size,
      type: iconType,
      "aria-label": name,
      color: iconCustomColor === null ? undefined : iconCustomColor
    });
  }

  return ___EmotionJSX("div", _extends({
    className: classes,
    style: avatarStyle,
    "aria-label": isDisabled ? undefined : name,
    role: isDisabled ? 'presentation' : 'img',
    title: name
  }, rest), content);
}; // TODO: Migrate to a service

EuiAvatar.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
         * Custom initials (max 2 characters).
         * By default will take the first character (of each word).
         */
  initials: PropTypes.string,

  /**
         * Specify how many characters to show (1 or 2).
         * By default, will show based on number of words (max first 2).
         */
  initialsLength: PropTypes.oneOf([1, 2]),

  /**
         * Path to an image to display instead of initials
         */
  imageUrl: PropTypes.string,

  /**
       * Any EUI glyph, logo or custom icon to display instead of initials
       */
  iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

  /**
       * Manually change icon size
       */
  iconSize: PropTypes.oneOf(["original", "s", "m", "l", "xl", "xxl"]),

  /**
       * Manually change icon color
       */
  iconColor: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.oneOf(["default", "primary", "success", "accent", "warning", "danger", "text", "subdued", "ghost", "inherit"]).isRequired]).isRequired, PropTypes.oneOf([null])]),

  /**
       * Full name of avatar for title attribute and calculating initial if not provided
       */
  name: PropTypes.string.isRequired,

  /**
       * Accepts hex values like `#FFFFFF`, `#000` otherwise a viz palette color will be assigned.
       * Or pass `'plain'` for an empty shade or `null` to remove entirely and the text/icon color will `inherit`
       */
  color: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.oneOf(["plain", null])]),

  /**
       * The type of avatar mainly controlling the shape.
       * `user` = circle
       * `space` = rounded square
       */
  type: PropTypes.oneOf(["space", "user"]),
  size: PropTypes.oneOf(["s", "m", "l", "xl"]),

  /**
       * Grays out the avatar to simulate being disabled
       */
  isDisabled: PropTypes.bool
};
export var checkValidColor = function checkValidColor(color) {
  var validHex = color && isValidHex(color) || color === 'plain';

  if (color && !validHex) {
    throw new Error('EuiAvatar needs to pass a valid color. This can either be a three ' + 'or six character hex value');
  }
};

function checkValidInitials(initials) {
  // Must be a string of 1 or 2 characters
  if (initials && initials.length > 2) {
    console.warn('EuiAvatar only accepts a max of 2 characters for the initials as a string. It is displaying only the first 2 characters.');
  }
}