function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { createElement } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { EuiNotificationEventMeta } from './notification_event_meta';
import { EuiNotificationEventMessages } from './notification_event_messages';
import { EuiNotificationEventReadButton } from './notification_event_read_button';
import { EuiButtonEmpty } from '../button';
import { EuiLink } from '../link';
import { useGeneratedHtmlId } from '../../services';
import { EuiNotificationEventReadIcon } from './notification_event_read_icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiNotificationEvent = function EuiNotificationEvent(_ref) {
  var id = _ref.id,
      type = _ref.type,
      severity = _ref.severity,
      badgeColor = _ref.badgeColor,
      iconType = _ref.iconType,
      iconAriaLabel = _ref.iconAriaLabel,
      time = _ref.time,
      title = _ref.title,
      isRead = _ref.isRead,
      primaryAction = _ref.primaryAction,
      primaryActionProps = _ref.primaryActionProps,
      messages = _ref.messages,
      onRead = _ref.onRead,
      onOpenContextMenu = _ref.onOpenContextMenu,
      onClickTitle = _ref.onClickTitle,
      onClickPrimaryAction = _ref.onClickPrimaryAction,
      _ref$headingLevel = _ref.headingLevel,
      headingLevel = _ref$headingLevel === void 0 ? 'h2' : _ref$headingLevel;
  var classes = classNames('euiNotificationEvent', {
    'euiNotificationEvent--withReadState': typeof isRead === 'boolean'
  });
  var classesTitle = classNames('euiNotificationEvent__title', {
    'euiNotificationEvent__title--isRead': isRead
  });
  var randomHeadingId = useGeneratedHtmlId();
  var titleProps = {
    id: randomHeadingId,
    className: classesTitle,
    'data-test-subj': "".concat(id, "-notificationEventTitle")
  };
  return ___EmotionJSX("article", {
    "aria-labelledby": randomHeadingId,
    className: classes,
    key: id
  }, typeof isRead === 'boolean' && ___EmotionJSX("div", {
    className: "euiNotificationEvent__readButton"
  }, !!onRead ? ___EmotionJSX(EuiNotificationEventReadButton, {
    isRead: isRead,
    onClick: function onClick() {
      return onRead(id, isRead);
    },
    eventName: title,
    id: id
  }) : ___EmotionJSX(EuiNotificationEventReadIcon, {
    id: id,
    isRead: isRead,
    eventName: title
  })), ___EmotionJSX("div", {
    className: "euiNotificationEvent__content"
  }, ___EmotionJSX(EuiNotificationEventMeta, {
    id: id,
    type: type,
    severity: severity,
    badgeColor: badgeColor,
    iconType: iconType,
    iconAriaLabel: iconAriaLabel,
    time: time,
    onOpenContextMenu: onOpenContextMenu ? function () {
      return onOpenContextMenu(id);
    } : undefined,
    eventName: title
  }), onClickTitle ? ___EmotionJSX(EuiLink, _extends({
    onClick: function onClick() {
      return onClickTitle(id);
    }
  }, titleProps), /*#__PURE__*/createElement(headingLevel, null, title)) : /*#__PURE__*/createElement(headingLevel, titleProps, title), ___EmotionJSX(EuiNotificationEventMessages, {
    messages: messages,
    eventName: title
  }), onClickPrimaryAction && primaryAction && ___EmotionJSX("div", {
    className: "euiNotificationEvent__primaryAction"
  }, ___EmotionJSX(EuiButtonEmpty, _extends({
    flush: "left",
    size: "s"
  }, primaryActionProps, {
    onClick: function onClick() {
      return onClickPrimaryAction === null || onClickPrimaryAction === void 0 ? void 0 : onClickPrimaryAction(id);
    },
    "data-test-subj": "".concat(id, "-notificationEventPrimaryAction")
  }), primaryAction))));
};
EuiNotificationEvent.propTypes = {
  /**
     * Type of event (e.g. "Alert", "Cloud", etc..). Shows inside a badge.
     */
  type: PropTypes.string.isRequired,

  /**
     * Type of severity (e.g. "Critical", "Warning", etc..). Shows as a text after the `type` following the format "Alert: Critical".
     */
  severity: PropTypes.string,

  /**
     * Accepts either our palette colors (primary, success ..etc) or a hex value `#FFFFFF`, `#000`.
     */
  badgeColor: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.oneOf(["default", "primary", "success", "accent", "warning", "danger", "text", "subdued", "ghost", "inherit"]).isRequired]),

  /**
     * The icon used to visually represent this data type. Accepts any `EuiIcon IconType`.
     */
  iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

  /**
     * Specify an `aria-label` for the icon.
     * If no `aria-label` is passed we assume the icon is purely decorative.
     */
  iconAriaLabel: PropTypes.string,

  /**
     * Indicates when the event was received.
     */
  time: PropTypes.node.isRequired,
  "aria-label": PropTypes.string,
  "aria-labelledby": PropTypes.string,

  /**
     * Size of the icon only.
     * This will not affect the overall size of the button
     */
  iconSize: PropTypes.oneOf(["original", "s", "m", "l", "xl", "xxl"]),

  /**
     * Sets the display style for matching other EuiButton types.
     * `base` is equivalent to a typical EuiButton
     * `fill` is equivalent to a filled EuiButton
     * `empty` (default) is equivalent to an EuiButtonEmpty
     */
  display: PropTypes.oneOf(["base", "empty", "fill"]),
  className: PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * A unique identifier
       */
  id: PropTypes.string.isRequired,

  /**
       * The title of the event.
       */
  title: PropTypes.string.isRequired,

  /**
       * The heading level of the title.
       */
  headingLevel: PropTypes.oneOf(["h2", "h3", "h4", "h5", "h6"]),

  /**
       * Returns the `id` and applies an `onClick` handler to the title.
       */
  onClickTitle: PropTypes.func,

  /**
       * The label of the primary action
       */
  primaryAction: PropTypes.string,

  /**
       * Apply more props to the `primaryAction` button. See #EuiPrimaryActionProps.
       */
  primaryActionProps: PropTypes.shape({
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
  }),

  /**
       * Returns the `id` and applies an `onClick` handler to the `primaryAction`.
       */
  onClickPrimaryAction: PropTypes.func,

  /**
       * Notification messages as an array of strings. More than one message wraps in an accordion.
       */
  messages: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,

  /**
       * Shows an indicator of the read state of the event. Leave as `undefined` to hide the indicator.
       */
  isRead: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.oneOf([undefined])]),

  /**
       * Returns the `id` and `isRead` state. Applies an `onClick` handler to the `read` indicator.
       */
  onRead: PropTypes.func,

  /**
       * Provided the `id` of the event must return an array of #EuiContextMenuItem elements.
       */
  onOpenContextMenu: PropTypes.func
};