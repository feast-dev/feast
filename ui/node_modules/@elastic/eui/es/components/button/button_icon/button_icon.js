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
import { getSecureRelForTarget } from '../../../services';
import { keysOf } from '../../common';
import { EuiIcon } from '../../icon';
import { validateHref } from '../../../services/security/href_validator';
import { jsx as ___EmotionJSX } from "@emotion/react";
var displayToClassNameMap = {
  base: null,
  empty: 'euiButtonIcon--empty',
  fill: 'euiButtonIcon--fill'
};
export var DISPLAYS = keysOf(displayToClassNameMap);
var colorToClassNameMap = {
  accent: 'euiButtonIcon--accent',
  danger: 'euiButtonIcon--danger',
  ghost: 'euiButtonIcon--ghost',
  primary: 'euiButtonIcon--primary',
  success: 'euiButtonIcon--success',
  text: 'euiButtonIcon--text',
  warning: 'euiButtonIcon--warning'
};
export var COLORS = keysOf(colorToClassNameMap);
var sizeToClassNameMap = {
  xs: 'euiButtonIcon--xSmall',
  s: 'euiButtonIcon--small',
  m: 'euiButtonIcon--medium'
};
export var SIZES = keysOf(sizeToClassNameMap);
export var EuiButtonIcon = function EuiButtonIcon(_ref) {
  var className = _ref.className,
      iconType = _ref.iconType,
      _ref$iconSize = _ref.iconSize,
      iconSize = _ref$iconSize === void 0 ? 'm' : _ref$iconSize,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'primary' : _ref$color,
      _isDisabled = _ref.isDisabled,
      disabled = _ref.disabled,
      href = _ref.href,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'button' : _ref$type,
      _ref$display = _ref.display,
      display = _ref$display === void 0 ? 'empty' : _ref$display,
      target = _ref.target,
      rel = _ref.rel,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'xs' : _ref$size,
      buttonRef = _ref.buttonRef,
      isSelected = _ref.isSelected,
      rest = _objectWithoutProperties(_ref, ["className", "iconType", "iconSize", "color", "isDisabled", "disabled", "href", "type", "display", "target", "rel", "size", "buttonRef", "isSelected"]);

  var isHrefValid = !href || validateHref(href);
  var isDisabled = _isDisabled || disabled || !isHrefValid;
  var ariaHidden = rest['aria-hidden'];
  var isAriaHidden = ariaHidden === 'true' || ariaHidden === true;

  if (!rest['aria-label'] && !rest['aria-labelledby'] && !isAriaHidden) {
    console.warn("EuiButtonIcon requires aria-label or aria-labelledby to be specified because icon-only\n      buttons are screen-reader-inaccessible without them.");
  }

  var classes = classNames('euiButtonIcon', {
    'euiButtonIcon-isDisabled': isDisabled
  }, colorToClassNameMap[color], display && displayToClassNameMap[display], size && sizeToClassNameMap[size], className); // Add an icon to the button if one exists.

  var buttonIcon;

  if (iconType) {
    buttonIcon = ___EmotionJSX(EuiIcon, {
      className: "euiButtonIcon__icon",
      type: iconType,
      size: iconSize,
      "aria-hidden": "true",
      color: "inherit" // forces the icon to inherit its parent color

    });
  } // <a> elements don't respect the `disabled` attribute. So if we're disabled, we'll just pretend
  // this is a button and piggyback off its disabled styles.


  if (href && !isDisabled) {
    var secureRel = getSecureRelForTarget({
      href: href,
      target: target,
      rel: rel
    });
    return ___EmotionJSX("a", _extends({
      tabIndex: isAriaHidden ? -1 : undefined,
      className: classes,
      href: href,
      target: target,
      rel: secureRel,
      ref: buttonRef
    }, rest), buttonIcon);
  }

  var buttonType;
  return ___EmotionJSX("button", _extends({
    tabIndex: isAriaHidden ? -1 : undefined,
    disabled: isDisabled,
    className: classes,
    "aria-pressed": isSelected,
    type: type,
    ref: buttonRef
  }, rest), buttonIcon);
};
EuiButtonIcon.propTypes = {
  type: PropTypes.oneOfType([PropTypes.string, PropTypes.oneOf(["submit", "reset", "button"])]),
  href: PropTypes.string,
  onClick: PropTypes.func,
  iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]).isRequired,

  /**
     * Any of the named color palette options.
     */
  color: PropTypes.oneOf(["primary", "accent", "success", "warning", "danger", "ghost", "text"]),
  "aria-label": PropTypes.string,
  "aria-labelledby": PropTypes.string,
  isDisabled: PropTypes.bool,

  /**
     * Overall size of button.
     * Matches the sizes of other EuiButtons
     */

  /**
     * Overall size of button.
     * Matches the sizes of other EuiButtons
     */
  size: PropTypes.oneOf(["xs", "s", "m"]),

  /**
     * Size of the icon only.
     * This will not affect the overall size of the button
     */
  iconSize: PropTypes.oneOf(["original", "s", "m", "l", "xl", "xxl"]),

  /**
     * Applies the boolean state as the `aria-pressed` property to create a toggle button.
     * *Only use when the readable text does not change between states.*
     */

  /**
     * Applies the boolean state as the `aria-pressed` property to create a toggle button.
     * *Only use when the readable text does not change between states.*
     */
  isSelected: PropTypes.bool,

  /**
     * Sets the display style for matching other EuiButton types.
     * `base` is equivalent to a typical EuiButton
     * `fill` is equivalent to a filled EuiButton
     * `empty` (default) is equivalent to an EuiButtonEmpty
     */

  /**
     * Sets the display style for matching other EuiButton types.
     * `base` is equivalent to a typical EuiButton
     * `fill` is equivalent to a filled EuiButton
     * `empty` (default) is equivalent to an EuiButtonEmpty
     */
  display: PropTypes.oneOf(["base", "empty", "fill"]),
  className: PropTypes.string,
  "data-test-subj": PropTypes.string,
  buttonRef: PropTypes.any
};