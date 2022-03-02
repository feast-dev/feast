function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

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
import React, { Fragment } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { keysOf } from '../../common';
import { getSecureRelForTarget } from '../../../services';
import { EuiToolTip } from '../../tool_tip';
import { EuiIcon } from '../../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
var colorToClassMap = {
  accent: 'euiBetaBadge--accent',
  subdued: 'euiBetaBadge--subdued',
  hollow: 'euiBetaBadge--hollow'
};
export var COLORS = keysOf(colorToClassMap);
export var sizeToClassMap = {
  s: 'euiBetaBadge--small',
  m: null
};
export var SIZES = keysOf(sizeToClassMap);
export var EuiBetaBadge = function EuiBetaBadge(_ref) {
  var className = _ref.className,
      label = _ref.label,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'hollow' : _ref$color,
      tooltipContent = _ref.tooltipContent,
      _ref$tooltipPosition = _ref.tooltipPosition,
      tooltipPosition = _ref$tooltipPosition === void 0 ? 'top' : _ref$tooltipPosition,
      title = _ref.title,
      iconType = _ref.iconType,
      onClick = _ref.onClick,
      onClickAriaLabel = _ref.onClickAriaLabel,
      href = _ref.href,
      rel = _ref.rel,
      target = _ref.target,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      rest = _objectWithoutProperties(_ref, ["className", "label", "color", "tooltipContent", "tooltipPosition", "title", "iconType", "onClick", "onClickAriaLabel", "href", "rel", "target", "size"]);

  var singleLetter = false;

  if (typeof label === 'string' && label.length === 1) {
    singleLetter = true;
  }

  var classes = classNames('euiBetaBadge', {
    'euiBetaBadge--iconOnly': iconType,
    'euiBetaBadge--singleLetter': singleLetter,
    'euiBetaBadge-isClickable': onClick || href
  }, colorToClassMap[color], sizeToClassMap[size], className);
  var icon;

  if (iconType) {
    icon = ___EmotionJSX(EuiIcon, {
      className: "euiBetaBadge__icon",
      type: iconType,
      size: size === 'm' ? 'm' : 's',
      "aria-hidden": "true",
      color: "inherit" // forces the icon to inherit its parent color

    });
  }

  var Element = href ? 'a' : 'button';
  var relObj = {};

  if (href) {
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

  var content;

  if (onClick || href) {
    content = ___EmotionJSX(Element, _extends({
      "aria-label": onClickAriaLabel,
      className: classes,
      title: typeof label === 'string' ? label : title
    }, relObj, rest), icon || label);

    if (tooltipContent) {
      return ___EmotionJSX(EuiToolTip, {
        position: tooltipPosition,
        content: tooltipContent,
        title: title || label
      }, content);
    } else {
      return ___EmotionJSX(Fragment, null, content);
    }
  } else {
    if (tooltipContent) {
      return ___EmotionJSX(EuiToolTip, {
        position: tooltipPosition,
        content: tooltipContent,
        title: title || label
      }, ___EmotionJSX("span", _extends({
        tabIndex: 0,
        className: classes,
        role: "button"
      }, rest), icon || label));
    } else {
      var spanTitle = title || label;

      if (spanTitle && typeof spanTitle !== 'string') {
        console.warn("Only string titles are permitted on badges that do not use tooltips. Found: ".concat(_typeof(spanTitle)));
      }

      return ___EmotionJSX("span", _extends({
        className: classes,
        title: spanTitle
      }, rest), icon || label);
    }
  }
};
EuiBetaBadge.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

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
  rel: PropTypes.string,

  /**
     * Supply an icon type if the badge should just be an icon
     */
  iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

  /**
     * One word label like "Beta" or "Lab"
     */

  /**
     * One word label like "Beta" or "Lab"
     */
  label: PropTypes.oneOfType([PropTypes.node.isRequired, PropTypes.oneOfType([PropTypes.node.isRequired, PropTypes.string.isRequired]).isRequired]).isRequired,

  /**
     * Content for the tooltip
     */
  tooltipContent: PropTypes.oneOfType([PropTypes.node, PropTypes.oneOfType([PropTypes.node, PropTypes.node.isRequired])]),

  /**
     * Custom position of the tooltip
     */
  tooltipPosition: PropTypes.oneOf(["top", "right", "bottom", "left"]),

  /**
     * Optional title will be supplied as tooltip title or title attribute
     * otherwise the label will be used
     */
  title: PropTypes.oneOfType([PropTypes.string, PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.string])]),

  /**
     * Accepts accent, subdued and hollow.
     */
  color: PropTypes.oneOf(["accent", "subdued", "hollow"]),
  size: PropTypes.oneOf(["s", "m"])
};