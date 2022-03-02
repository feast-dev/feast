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
import { keysOf } from '../common';
import { EuiTitle } from '../title';
import { EuiFlexGroup, EuiFlexItem } from '../flex';
import { EuiSpacer } from '../spacer';
import { EuiIcon } from '../icon';
import { isNamedColor } from '../icon/named_colors';
import { EuiText, EuiTextColor } from '../text';
import { EuiPanel } from '../panel/panel';
import { jsx as ___EmotionJSX } from "@emotion/react";
var paddingSizeToClassNameMap = {
  none: null,
  s: 'euiEmptyPrompt--paddingSmall',
  m: 'euiEmptyPrompt--paddingMedium',
  l: 'euiEmptyPrompt--paddingLarge'
};
export var PADDING_SIZES = keysOf(paddingSizeToClassNameMap);
export var EuiEmptyPrompt = function EuiEmptyPrompt(_ref) {
  var icon = _ref.icon,
      iconType = _ref.iconType,
      _iconColor = _ref.iconColor,
      title = _ref.title,
      _ref$titleSize = _ref.titleSize,
      titleSize = _ref$titleSize === void 0 ? 'm' : _ref$titleSize,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'l' : _ref$paddingSize,
      body = _ref.body,
      actions = _ref.actions,
      className = _ref.className,
      _ref$layout = _ref.layout,
      layout = _ref$layout === void 0 ? 'vertical' : _ref$layout,
      hasBorder = _ref.hasBorder,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'transparent' : _ref$color,
      footer = _ref.footer,
      rest = _objectWithoutProperties(_ref, ["icon", "iconType", "iconColor", "title", "titleSize", "paddingSize", "body", "actions", "className", "layout", "hasBorder", "color", "footer"]);

  var isVerticalLayout = layout === 'vertical'; // Default the iconColor to `subdued`,
  // otherwise try to match the iconColor with the panel color unless iconColor is specified

  var iconColor = _iconColor !== null && _iconColor !== void 0 ? _iconColor : isNamedColor(color) ? color : 'subdued';
  var iconNode = iconType ? ___EmotionJSX(EuiIcon, {
    type: iconType,
    size: "xxl",
    color: iconColor
  }) : icon;
  var titleNode;
  var bodyNode;

  if (body || title) {
    if (title) {
      titleNode = ___EmotionJSX(EuiTitle, {
        size: titleSize
      }, title);
    }

    if (body) {
      bodyNode = ___EmotionJSX(EuiTextColor, {
        color: "subdued"
      }, title && ___EmotionJSX(EuiSpacer, {
        size: "m"
      }), ___EmotionJSX(EuiText, null, body));
    }
  }

  var actionsNode;

  if (actions) {
    var actionsRow;

    if (Array.isArray(actions)) {
      actionsRow = ___EmotionJSX(EuiFlexGroup, {
        className: "euiEmptyPrompt__actions",
        gutterSize: "m",
        alignItems: "center",
        justifyContent: "center",
        direction: isVerticalLayout ? 'column' : 'row'
      }, actions.map(function (action, index) {
        return ___EmotionJSX(EuiFlexItem, {
          key: index,
          grow: false
        }, action);
      }));
    } else {
      actionsRow = actions;
    }

    actionsNode = ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiSpacer, {
      size: "l"
    }), actionsRow);
  }

  var contentNodes = ___EmotionJSX(React.Fragment, null, titleNode, bodyNode, actionsNode);

  var classes = classNames('euiEmptyPrompt', ["euiEmptyPrompt--".concat(layout)], paddingSizeToClassNameMap[paddingSize], className);

  var panelProps = _objectSpread({
    className: classes,
    color: color,
    paddingSize: 'none',
    hasBorder: hasBorder
  }, rest);

  return ___EmotionJSX(EuiPanel, panelProps, ___EmotionJSX("div", {
    className: "euiEmptyPrompt__main"
  }, iconNode && ___EmotionJSX("div", {
    className: "euiEmptyPrompt__icon"
  }, iconNode), ___EmotionJSX("div", {
    className: "euiEmptyPrompt__content"
  }, ___EmotionJSX("div", {
    className: "euiEmptyPrompt__contentInner"
  }, contentNodes))), footer && ___EmotionJSX("div", {
    className: "euiEmptyPrompt__footer"
  }, footer));
};
EuiEmptyPrompt.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  element: PropTypes.oneOf(["div"]),

  /**
     * Adds a medium shadow to the panel;
     * Only works when `color="plain"`
     */
  hasShadow: PropTypes.bool,

  /**
     * Adds a slight 1px border on all edges.
     * Only works when `color="plain | transparent"`
     * Default is `undefined` and will default to that theme's panel style
     */
  hasBorder: PropTypes.bool,

  /**
     * Background color of the panel;
     * Usually a lightened form of the brand colors
     */
  color: PropTypes.any,

  /*
       * Accepts any [EuiIcon.type](#/display/icons)
       */
  iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

  /**
       * Color for `iconType` when passed as an `IconType`
       */
  iconColor: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.oneOf(["default", "primary", "success", "accent", "warning", "danger", "text", "subdued", "ghost", "inherit"]).isRequired]),

  /**
       * Custom icon replacing the one generated by `iconType`
       */
  icon: PropTypes.node,

  /**
       * Requires passing a single element that gets wrapped in an EuiTitle.
       * Recommendation is a heading, preferrably an `<h2>` if in its own section
       */
  title: PropTypes.element,

  /**
       * Choose from one of the `EuiTitle.size` options
       */
  titleSize: PropTypes.oneOf(["xxxs", "xxs", "xs", "s", "m", "l"]),

  /**
       * Gets wrapped in a subdued EuiText block.
       * Recommendation is to pass typical text elements like `<p>`
       */
  body: PropTypes.node,

  /**
       * Pass a single or an array of actions (buttons) that get stacked at the bottom.
       * Recommendation is to pass the primary action first and secondary actions as empty buttons
       */
  actions: PropTypes.node,

  /**
       * Optionally provide a footer. Accepts any combination of elements.
       */
  footer: PropTypes.node,

  /**
       * Sets the layout. When `horizontal` the icon goes to the right column.
       */
  layout: PropTypes.oneOf(["vertical", "horizontal"]),

  /**
       * Padding applied around the content and footer.
       */
  paddingSize: PropTypes.any
};