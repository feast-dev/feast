function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

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
import { useEuiI18n } from '../i18n';
import { EuiNotificationBadge } from '../badge/notification_badge';
import { EuiButtonEmpty } from '../button/button_empty';
import { useInnerText } from '../inner_text';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFilterButton = function EuiFilterButton(_ref) {
  var children = _ref.children,
      className = _ref.className,
      iconType = _ref.iconType,
      _ref$iconSide = _ref.iconSide,
      iconSide = _ref$iconSide === void 0 ? 'right' : _ref$iconSide,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'text' : _ref$color,
      hasActiveFilters = _ref.hasActiveFilters,
      numFilters = _ref.numFilters,
      numActiveFilters = _ref.numActiveFilters,
      isDisabled = _ref.isDisabled,
      isSelected = _ref.isSelected,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'button' : _ref$type,
      _ref$grow = _ref.grow,
      grow = _ref$grow === void 0 ? true : _ref$grow,
      noDivider = _ref.noDivider,
      withNext = _ref.withNext,
      textProps = _ref.textProps,
      rest = _objectWithoutProperties(_ref, ["children", "className", "iconType", "iconSide", "color", "hasActiveFilters", "numFilters", "numActiveFilters", "isDisabled", "isSelected", "type", "grow", "noDivider", "withNext", "textProps"]);

  var numFiltersDefined = numFilters != null; // != instead of !== to allow for null and undefined

  var numActiveFiltersDefined = numActiveFilters != null && numActiveFilters > 0;
  var classes = classNames('euiFilterButton', {
    'euiFilterButton-isSelected': isSelected,
    'euiFilterButton-hasActiveFilters': hasActiveFilters,
    'euiFilterButton-hasNotification': numFiltersDefined,
    'euiFilterButton--hasIcon': iconType,
    'euiFilterButton--noGrow': !grow,
    'euiFilterButton--withNext': noDivider || withNext
  }, className);
  var buttonTextClassNames = classNames({
    'euiFilterButton__text-hasNotification': numFiltersDefined || numActiveFilters
  }, textProps && textProps.className);
  var showBadge = numFiltersDefined || numActiveFiltersDefined;
  var badgeCount = numActiveFilters || numFilters;
  var activeBadgeLabel = useEuiI18n('euiFilterButton.filterBadgeActiveAriaLabel', '{count} active filters', {
    count: badgeCount
  });
  var availableBadgeLabel = useEuiI18n('euiFilterButton.filterBadgeAvailableAriaLabel', '{count} available filters', {
    count: badgeCount
  });

  var badgeContent = showBadge && ___EmotionJSX(EuiNotificationBadge, {
    className: "euiFilterButton__notification",
    size: "m",
    "aria-label": hasActiveFilters ? activeBadgeLabel : availableBadgeLabel,
    color: isDisabled || !hasActiveFilters ? 'subdued' : 'accent'
  }, badgeCount);

  var dataText;

  if (typeof children === 'string') {
    dataText = children;
  }

  var _useInnerText = useInnerText(),
      _useInnerText2 = _slicedToArray(_useInnerText, 2),
      ref = _useInnerText2[0],
      innerText = _useInnerText2[1];

  var buttonContents = ___EmotionJSX(Fragment, null, ___EmotionJSX("span", {
    ref: ref,
    className: "euiFilterButton__textShift",
    "data-text": dataText || innerText,
    title: dataText || innerText
  }, children), badgeContent);

  return ___EmotionJSX(EuiButtonEmpty, _extends({
    className: classes,
    color: color,
    isDisabled: isDisabled,
    iconSide: iconSide,
    iconType: iconType,
    type: type,
    textProps: _objectSpread(_objectSpread({}, textProps), {}, {
      className: buttonTextClassNames
    })
  }, rest), buttonContents);
};
EuiFilterButton.propTypes = {
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
     * Applies a visual state to the button useful when using with a popover.
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
  "data-test-subj": PropTypes.string,

  /**
     * Bolds the button if true
     */
  hasActiveFilters: PropTypes.bool,

  /**
     * Pass the total number of filters available and it will
     * add a subdued notification badge showing the number
     */
  numFilters: PropTypes.number,

  /**
     * Pass the number of selected filters and it will
     * add a bright notification badge showing the number
     */
  numActiveFilters: PropTypes.number,

  /**
     * Should the button grow to fill its container, best used for dropdown buttons
     */
  grow: PropTypes.bool,

  /**
     * Remove border after button, good for opposite filters
     */
  withNext: PropTypes.bool,

  /**
     * _DEPRECATED: use `withNext`_
     * Remove border after button, good for opposite filters
     */
  noDivider: PropTypes.bool
};