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
import { useGeneratedHtmlId } from '../../../services';
import { EuiAccordion } from '../../accordion';
import { EuiIcon } from '../../icon';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { EuiTitle } from '../../title';
import { jsx as ___EmotionJSX } from "@emotion/react";
var backgroundToClassNameMap = {
  none: '',
  light: 'euiCollapsibleNavGroup--light',
  dark: 'euiCollapsibleNavGroup--dark'
};
export var BACKGROUNDS = Object.keys(backgroundToClassNameMap);
export var EuiCollapsibleNavGroup = function EuiCollapsibleNavGroup(_ref) {
  var className = _ref.className,
      children = _ref.children,
      id = _ref.id,
      title = _ref.title,
      iconType = _ref.iconType,
      _ref$iconSize = _ref.iconSize,
      iconSize = _ref$iconSize === void 0 ? 'l' : _ref$iconSize,
      _ref$background = _ref.background,
      background = _ref$background === void 0 ? 'none' : _ref$background,
      _ref$isCollapsible = _ref.isCollapsible,
      isCollapsible = _ref$isCollapsible === void 0 ? false : _ref$isCollapsible,
      _ref$titleElement = _ref.titleElement,
      titleElement = _ref$titleElement === void 0 ? 'h3' : _ref$titleElement,
      _ref$titleSize = _ref.titleSize,
      titleSize = _ref$titleSize === void 0 ? 'xxs' : _ref$titleSize,
      iconProps = _ref.iconProps,
      rest = _objectWithoutProperties(_ref, ["className", "children", "id", "title", "iconType", "iconSize", "background", "isCollapsible", "titleElement", "titleSize", "iconProps"]);

  var groupID = useGeneratedHtmlId({
    conditionalId: id
  });
  var titleID = "".concat(groupID, "__title");
  var classes = classNames('euiCollapsibleNavGroup', backgroundToClassNameMap[background], {
    'euiCollapsibleNavGroup--withHeading': title
  }, className); // Warn if consumer passes an iconType without a title

  if (iconType && !title) {
    console.warn('EuiCollapsibleNavGroup will not render an icon without `title`.');
  }

  var content = children && ___EmotionJSX("div", {
    className: "euiCollapsibleNavGroup__children"
  }, children);

  var headingClasses = 'euiCollapsibleNavGroup__heading';
  var TitleElement = titleElement;
  var titleContent = title ? ___EmotionJSX(EuiFlexGroup, {
    gutterSize: "m",
    alignItems: "center",
    responsive: false
  }, iconType && ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, ___EmotionJSX(EuiIcon, _extends({}, iconProps, {
    type: iconType,
    size: iconSize
  }))), ___EmotionJSX(EuiFlexItem, null, ___EmotionJSX(EuiTitle, {
    size: titleSize
  }, ___EmotionJSX(TitleElement, {
    id: titleID,
    className: "euiCollapsibleNavGroup__title"
  }, title)))) : undefined;

  if (isCollapsible && title) {
    return ___EmotionJSX(EuiAccordion, _extends({
      id: groupID,
      className: classes,
      buttonClassName: headingClasses,
      buttonContent: titleContent,
      initialIsOpen: true,
      arrowDisplay: "right",
      arrowProps: {
        color: background === 'dark' ? 'ghost' : 'text'
      }
    }, rest), content);
  } else {
    return ___EmotionJSX("div", _extends({
      id: groupID,
      className: classes
    }, rest), titleContent && ___EmotionJSX("div", {
      className: headingClasses
    }, titleContent), content);
  }
};
EuiCollapsibleNavGroup.propTypes = {
  /**
     * ReactNode to render as this component's content
     */

  /**
     * ReactNode to render as this component's content
     */
  children: PropTypes.node,

  /**
     * Sits left of the `title` and only when `title` is present
     */
  iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

  /**
     * Change the size of the icon in the `title`
     */
  iconSize: PropTypes.oneOf(["original", "s", "m", "l", "xl", "xxl"]),

  /**
     * Further extend the props applied to EuiIcon
     */
  iconProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,

    /**
         * One of EUI's color palette or a valid CSS color value https://developer.mozilla.org/en-US/docs/Web/CSS/color_value.
         * Note that coloring only works if your SVG is removed of fill attributes.
         */
    color: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.oneOf(["default", "primary", "success", "accent", "warning", "danger", "text", "subdued", "ghost", "inherit"]).isRequired]),

    /**
         * Descriptive title for naming the icon based on its use
         */
    title: PropTypes.string,

    /**
         * A unique identifier for the title element
         */
    titleId: PropTypes.string,

    /**
         * Its value should be one or more element IDs
         */
    "aria-labelledby": PropTypes.string,

    /**
         * Callback when the icon has been loaded & rendered
         */
    onIconLoad: PropTypes.func
  }),

  /**
     * Optionally provide an id, otherwise one will be created
     */

  /**
     * Optionally provide an id, otherwise one will be created
     */
  id: PropTypes.string,

  /**
     * Adds a background color to the entire group,
     * applying the correct text color to the `title` only
     */

  /**
     * Adds a background color to the entire group,
     * applying the correct text color to the `title` only
     */
  background: PropTypes.oneOf(["none", "light", "dark"]),

  /**
     * Determines the title's heading element
     */

  /**
     * Determines the title's heading element
     */
  titleElement: PropTypes.oneOf(["h2", "h3", "h4", "h5", "h6", "span"]),

  /**
     * Title sizing equivalent to EuiTitle, but only `s` and smaller
     */

  /**
     * Title sizing equivalent to EuiTitle, but only `s` and smaller
     */
  titleSize: PropTypes.any,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Applied to the entire .euiAccordion wrapper.
       * When using `fieldset`, it will enforce `buttonElement = legend` as well.
       */
  element: PropTypes.oneOf(["div", "fieldset"]),

  /**
       * Class that will apply to the trigger for the accordion.
       */
  buttonClassName: PropTypes.string,

  /**
       * Apply more props to the triggering button
       */
  buttonProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }),

  /**
       * Class that will apply to the trigger content for the accordion.
       */
  buttonContentClassName: PropTypes.string,

  /**
       * The content of the clickable trigger
       */
  buttonContent: PropTypes.node,

  /**
       * Applied to the main button receiving the `onToggle` event.
       * Anything other than the default `button` does not support removing the arrow display (for accessibility of focus).
       */
  buttonElement: PropTypes.oneOf(["div", "legend", "button"]),

  /**
       * Extra props to pass to the EuiButtonIcon containing the arrow.
       */
  arrowProps: PropTypes.any,

  /**
       * Will appear right aligned against the button. Useful for separate actions like deletions.
       */
  extraAction: PropTypes.node,

  /**
       * The accordion will start in the open state.
       */
  initialIsOpen: PropTypes.bool,

  /**
       * Optional callback method called on open and close with a single `isOpen` parameter
       */
  onToggle: PropTypes.func,

  /**
       * The padding around the exposed accordion content.
       */
  paddingSize: PropTypes.oneOf(["none", "xs", "s", "m", "l", "xl"]),

  /**
       * Placement of the arrow indicator, or 'none' to hide it.
       */
  arrowDisplay: PropTypes.oneOf(["left", "right", "none"]),

  /**
       * Control the opening of accordion via prop
       */
  forceState: PropTypes.oneOf(["closed", "open"]),

  /**
       * Change `extraAction` and children into a loading spinner
       */
  isLoading: PropTypes.bool,

  /**
       * Choose whether the loading message replaces the content. Customize the message by passing a node
       */
  isLoadingMessage: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.node.isRequired]),

  /**
     * If `true`, wraps children in the body of an accordion,
     * requiring the prop `title` to be used as the button.
     * When `false`, simply renders a div without any accordion functionality.
     */

  /**
       * If `true`, wraps children in the body of an accordion,
       * requiring the prop `title` to be used as the button.
       * When `false`, simply renders a div without any accordion functionality.
       */
  isCollapsible: PropTypes.oneOfType([PropTypes.oneOf([true]).isRequired, PropTypes.oneOf([false])]),

  /**
     * The title gets wrapped in the appropriate heading level
     * with the option to add an iconType
     */

  /**
       * The title gets wrapped in the appropriate heading level
       * with the option to add an iconType
       */
  title: PropTypes.oneOfType([PropTypes.node.isRequired, PropTypes.node])
};