function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

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
import { EuiButtonIcon } from '../button';
import { EuiIcon } from '../icon';
import { EuiToolTip } from '../tool_tip';
import { useInnerText } from '../inner_text';
import { getSecureRelForTarget } from '../../services';
import { validateHref } from '../../services/security/href_validator';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  xs: 'euiListGroupItem--xSmall',
  s: 'euiListGroupItem--small',
  m: 'euiListGroupItem--medium',
  l: 'euiListGroupItem--large'
};
export var SIZES = Object.keys(sizeToClassNameMap);
var colorToClassNameMap = {
  inherit: '',
  primary: 'euiListGroupItem--primary',
  text: 'euiListGroupItem--text',
  subdued: 'euiListGroupItem--subdued',
  ghost: 'euiListGroupItem--ghost'
};
export var COLORS = Object.keys(colorToClassNameMap);
export var EuiListGroupItem = function EuiListGroupItem(_ref) {
  var label = _ref.label,
      _ref$isActive = _ref.isActive,
      isActive = _ref$isActive === void 0 ? false : _ref$isActive,
      _ref$isDisabled = _ref.isDisabled,
      _isDisabled = _ref$isDisabled === void 0 ? false : _ref$isDisabled,
      href = _ref.href,
      target = _ref.target,
      rel = _ref.rel,
      className = _ref.className,
      iconType = _ref.iconType,
      icon = _ref.icon,
      iconProps = _ref.iconProps,
      extraAction = _ref.extraAction,
      onClick = _ref.onClick,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'inherit' : _ref$color,
      _ref$showToolTip = _ref.showToolTip,
      showToolTip = _ref$showToolTip === void 0 ? false : _ref$showToolTip,
      wrapText = _ref.wrapText,
      buttonRef = _ref.buttonRef,
      rest = _objectWithoutProperties(_ref, ["label", "isActive", "isDisabled", "href", "target", "rel", "className", "iconType", "icon", "iconProps", "extraAction", "onClick", "size", "color", "showToolTip", "wrapText", "buttonRef"]);

  var isHrefValid = !href || validateHref(href);
  var isDisabled = _isDisabled || !isHrefValid;
  var classes = classNames('euiListGroupItem', sizeToClassNameMap[size], colorToClassNameMap[color], {
    'euiListGroupItem-isActive': isActive,
    'euiListGroupItem-isDisabled': isDisabled,
    'euiListGroupItem-isClickable': href || onClick,
    'euiListGroupItem-hasExtraAction': extraAction,
    'euiListGroupItem--wrapText': wrapText
  }, className);
  var iconNode;

  if (iconType) {
    iconNode = ___EmotionJSX(EuiIcon, _extends({
      color: "inherit" // forces the icon to inherit its parent color

    }, iconProps, {
      type: iconType,
      className: classNames('euiListGroupItem__icon', iconProps === null || iconProps === void 0 ? void 0 : iconProps.className)
    }));

    if (icon) {
      console.warn('Both `iconType` and `icon` were passed to EuiListGroupItem but only one can exist. The `iconType` was used.');
    }
  } else if (icon) {
    iconNode = /*#__PURE__*/React.cloneElement(icon, {
      className: classNames('euiListGroupItem__icon', icon.props.className)
    });
  }

  var extraActionNode;

  if (extraAction) {
    var _iconType = extraAction.iconType,
        alwaysShow = extraAction.alwaysShow,
        _className = extraAction.className,
        actionIsDisabled = extraAction.isDisabled,
        _rest = _objectWithoutProperties(extraAction, ["iconType", "alwaysShow", "className", "isDisabled"]);

    var extraActionClasses = classNames('euiListGroupItem__extraAction', {
      'euiListGroupItem__extraAction-alwaysShow': alwaysShow
    }, _className);
    extraActionNode = ___EmotionJSX(EuiButtonIcon, _extends({
      className: extraActionClasses,
      iconType: _iconType
    }, _rest, {
      disabled: isDisabled || actionIsDisabled
    }));
  } // Only add the label as the title attribute if it's possibly truncated
  // Also ensure the value of the title attribute is a string


  var _useInnerText = useInnerText(),
      _useInnerText2 = _slicedToArray(_useInnerText, 2),
      ref = _useInnerText2[0],
      innerText = _useInnerText2[1];

  var shouldRenderTitle = !wrapText && !showToolTip;
  var labelContent = shouldRenderTitle ? ___EmotionJSX("span", {
    ref: ref,
    className: "euiListGroupItem__label",
    title: typeof label === 'string' ? label : innerText
  }, label) : ___EmotionJSX("span", {
    className: "euiListGroupItem__label"
  }, label); // Handle the variety of interaction behavior

  var itemContent;
  var secureRel = getSecureRelForTarget({
    href: href,
    rel: rel,
    target: target
  });

  if (href && !isDisabled) {
    itemContent = ___EmotionJSX("a", _extends({
      className: "euiListGroupItem__button",
      href: href,
      target: target,
      rel: secureRel,
      onClick: onClick
    }, rest), iconNode, labelContent);
  } else if (href && isDisabled || onClick) {
    itemContent = ___EmotionJSX("button", _extends({
      type: "button",
      className: "euiListGroupItem__button",
      disabled: isDisabled,
      onClick: onClick,
      ref: buttonRef
    }, rest), iconNode, labelContent);
  } else {
    itemContent = ___EmotionJSX("span", _extends({
      className: "euiListGroupItem__text"
    }, rest), iconNode, labelContent);
  }

  if (showToolTip) {
    itemContent = ___EmotionJSX("li", {
      className: classes
    }, ___EmotionJSX(EuiToolTip, {
      anchorClassName: "euiListGroupItem__tooltip",
      content: label,
      position: "right",
      delay: "long"
    }, itemContent));
  } else {
    itemContent = ___EmotionJSX("li", {
      className: classes
    }, itemContent, extraActionNode);
  }

  return ___EmotionJSX(Fragment, null, itemContent);
};
EuiListGroupItem.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Size of the label text
       */
  size: PropTypes.oneOf(["xs", "s", "m", "l"]),

  /**
       * By default the item will inherit the color of its wrapper (button/link/span),
       * otherwise pass one of the acceptable options
       */
  color: PropTypes.oneOf(["inherit", "primary", "text", "subdued", "ghost"]),

  /**
       * Content to be displayed in the list item
       */
  label: PropTypes.node.isRequired,

  /**
       * Apply styles indicating an item is active
       */
  isActive: PropTypes.bool,

  /**
       * Apply styles indicating an item is disabled
       */
  isDisabled: PropTypes.bool,

  /**
       * Make the list item label a link.
       * While permitted, `href` and `onClick` should not be used together in most cases and may create problems.
       */
  href: PropTypes.string,
  target: PropTypes.string,
  rel: PropTypes.string,

  /**
       * Adds `EuiIcon` of `EuiIcon.type`
       */
  iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

  /**
       * Further extend the props applied to EuiIcon
       */
  iconProps: PropTypes.any,

  /**
       * Custom node to pass as the icon. Cannot be used in conjunction
       * with `iconType` and `iconProps`.
       */
  icon: PropTypes.element,

  /**
       * Display tooltip on list item
       */
  showToolTip: PropTypes.bool,

  /**
       * Adds an `EuiButtonIcon` to the right side of the item; `iconType` is required;
       * pass `alwaysShow` if you don't want the default behavior of only showing on hover
       */
  extraAction: PropTypes.shape({
    type: PropTypes.oneOf(["submit", "reset", "button"]),
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
    isSelected: PropTypes.bool,

    /**
       * Sets the display style for matching other EuiButton types.
       * `base` is equivalent to a typical EuiButton
       * `fill` is equivalent to a filled EuiButton
       * `empty` (default) is equivalent to an EuiButtonEmpty
       */
    display: PropTypes.oneOf(["base", "empty", "fill"]),
    className: PropTypes.string,
    "data-test-subj": PropTypes.string,
    buttonRef: PropTypes.any,
    alwaysShow: PropTypes.bool
  }),

  /**
       * Make the list item label a button.
       * While permitted, `href` and `onClick` should not be used together in most cases and may create problems.
       */
  onClick: PropTypes.func,

  /**
       * Allow link text to wrap
       */
  wrapText: PropTypes.bool,

  /**
       * Pass-through ref reference specifically for targeting
       * instances where the item content is rendered as a `button`
       */
  buttonRef: PropTypes.any
};