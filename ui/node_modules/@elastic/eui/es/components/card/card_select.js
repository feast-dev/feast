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
import { EuiButtonEmpty } from '../button/button_empty';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiCardSelect = function EuiCardSelect(_ref) {
  var className = _ref.className,
      _ref$isSelected = _ref.isSelected,
      isSelected = _ref$isSelected === void 0 ? false : _ref$isSelected,
      isDisabled = _ref.isDisabled,
      color = _ref.color,
      children = _ref.children,
      rest = _objectWithoutProperties(_ref, ["className", "isSelected", "isDisabled", "color", "children"]);

  var child = euiCardSelectableText(isSelected, isDisabled, children);
  var selectClasses = classNames('euiCardSelect', "euiCardSelect--".concat(euiCardSelectableColor(color, isSelected)), className);
  return ___EmotionJSX(EuiButtonEmpty, _extends({
    className: selectClasses,
    color: color || 'text',
    size: "xs",
    isDisabled: isDisabled,
    iconType: isSelected ? 'check' : undefined,
    role: "switch",
    "aria-checked": isSelected
  }, rest), child);
};
EuiCardSelect.propTypes = {
  href: PropTypes.string,
  onClick: PropTypes.func,

  /**
     * Any of our named colors
     */

  /**
     * Any of our named colors
     */
  color: PropTypes.oneOf(["primary", "danger", "text", "ghost", "success", "warning"]),
  size: PropTypes.oneOf(["xs", "s", "m"]),

  /**
     * Ensure the text of the button sits flush to the left, right, or both sides of its container
     */

  /**
     * Ensure the text of the button sits flush to the left, right, or both sides of its container
     */
  flush: PropTypes.oneOf(["left", "right", "both"]),

  /**
     * `disabled` is also allowed
     */

  /**
     * `disabled` is also allowed
     */
  isDisabled: PropTypes.bool,

  /**
     * Force disables the button and changes the icon to a loading spinner
     */

  /**
     * Force disables the button and changes the icon to a loading spinner
     */
  isLoading: PropTypes.bool,

  /**
     * Is in the selected state
     */

  /**
     * Applies the boolean state as the `aria-pressed` property to create a toggle button.
     * *Only use when the readable text does not change between states.*
     */

  /**
     * Applies the boolean state as the `aria-pressed` property to create a toggle button.
     * *Only use when the readable text does not change between states.*
     */
  isSelected: PropTypes.bool,
  target: PropTypes.string,
  rel: PropTypes.string,
  type: PropTypes.oneOf(["button", "submit"]),
  buttonRef: PropTypes.any,

  /**
     * Object of props passed to the <span/> wrapping the button's content
     */
  contentProps: PropTypes.any,

  /**
     * Any `type` accepted by EuiIcon
     */
  iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

  /**
     * Can only be one side `left` or `right`
     */
  iconSide: PropTypes.oneOf(["left", "right"]),

  /**
     * Object of props passed to the <span/> wrapping the content's text/children only (not icon)
     */
  textProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,
    ref: PropTypes.any,
    "data-text": PropTypes.string
  }),
  iconSize: PropTypes.oneOf(["s", "m"]),
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};

function euiCardSelectableText(isSelected, isDisabled, children) {
  if (children) {
    return children;
  }

  var text;

  if (isSelected) {
    text = ___EmotionJSX(EuiI18n, {
      token: "euiCardSelect.selected",
      default: "Selected"
    });
  } else if (isDisabled) {
    text = ___EmotionJSX(EuiI18n, {
      token: "euiCardSelect.unavailable",
      default: "Unavailable"
    });
  } else {
    text = ___EmotionJSX(EuiI18n, {
      token: "euiCardSelect.select",
      default: "Select"
    });
  }

  return text;
}

export function euiCardSelectableColor(color, isSelected) {
  var calculatedColor;

  if (color) {
    calculatedColor = color;
  } else if (isSelected) {
    calculatedColor = 'success';
  } else {
    calculatedColor = 'text';
  }

  return calculatedColor;
}