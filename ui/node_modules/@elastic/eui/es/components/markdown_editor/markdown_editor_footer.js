function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useState, useMemo, Fragment, forwardRef } from 'react';
import PropTypes from "prop-types";
import { EuiLoadingSpinner } from '../loading';
import { EuiButton, EuiButtonEmpty, EuiButtonIcon } from '../button';
import { EuiTitle } from '../title';
import { EuiModal, EuiModalBody, EuiModalFooter, EuiModalHeader } from '../modal';
import { EuiI18n, useEuiI18n } from '../i18n';
import { EuiPopover, EuiPopoverTitle } from '../popover';
import { EuiText } from '../text';
import { EuiSpacer } from '../spacer';
import { EuiToolTip } from '../tool_tip'; // @ts-ignore a react svg

import MarkdownLogo from './icons/markdown_logo';
import { EuiHorizontalRule } from '../horizontal_rule';
import { EuiLink } from '../link';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiMarkdownEditorFooter = /*#__PURE__*/forwardRef(function (props, ref) {
  var uiPlugins = props.uiPlugins,
      isUploadingFiles = props.isUploadingFiles,
      openFiles = props.openFiles,
      errors = props.errors,
      hasUnacceptedItems = props.hasUnacceptedItems,
      dropHandlers = props.dropHandlers;

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isShowingHelpModal = _useState2[0],
      setIsShowingHelpModal = _useState2[1];

  var _useState3 = useState(false),
      _useState4 = _slicedToArray(_useState3, 2),
      isShowingHelpPopover = _useState4[0],
      setIsShowingHelpPopover = _useState4[1];

  var _useState5 = useState(false),
      _useState6 = _slicedToArray(_useState5, 2),
      isPopoverOpen = _useState6[0],
      setIsPopoverOpen = _useState6[1];

  var onButtonClick = function onButtonClick() {
    return setIsPopoverOpen(function (isPopoverOpen) {
      return !isPopoverOpen;
    });
  };

  var closePopover = function closePopover() {
    return setIsPopoverOpen(false);
  };

  var uploadButton;
  var supportedFileTypes = useMemo(function () {
    return dropHandlers.map(function (_ref) {
      var supportedFiles = _ref.supportedFiles;
      return supportedFiles.join(', ');
    }).sort().join(', ');
  }, [dropHandlers]);
  var ariaLabels = {
    uploadingFiles: useEuiI18n('euiMarkdownEditorFooter.uploadingFiles', 'Click to upload files'),
    openUploadModal: useEuiI18n('euiMarkdownEditorFooter.openUploadModal', 'Open upload files modal'),
    unsupportedFileType: useEuiI18n('euiMarkdownEditorFooter.unsupportedFileType', 'File type not supported'),
    supportedFileTypes: useEuiI18n('euiMarkdownEditorFooter.supportedFileTypes', 'Supported files: {supportedFileTypes}', {
      supportedFileTypes: supportedFileTypes
    }),
    showSyntaxErrors: useEuiI18n('euiMarkdownEditorFooter.showSyntaxErrors', 'Show errors'),
    showMarkdownHelp: useEuiI18n('euiMarkdownEditorFooter.showMarkdownHelp', 'Show markdown help')
  };
  var syntaxTitle = useEuiI18n('euiMarkdownEditorFooter.syntaxTitle', 'Syntax help');

  if (isUploadingFiles) {
    uploadButton = ___EmotionJSX(EuiButtonIcon, {
      size: "s",
      iconType: EuiLoadingSpinner,
      "aria-label": ariaLabels.uploadingFiles
    });
  } else if (dropHandlers.length > 0 && hasUnacceptedItems) {
    uploadButton = ___EmotionJSX(EuiToolTip, {
      content: ariaLabels.supportedFileTypes
    }, ___EmotionJSX(EuiButtonEmpty, {
      className: "euiMarkdownEditorFooter__uploadError",
      autoFocus: true,
      size: "s",
      iconType: "paperClip",
      color: "danger",
      "aria-label": "".concat(ariaLabels.unsupportedFileType, ". ").concat(ariaLabels.supportedFileTypes, ". ").concat(ariaLabels.uploadingFiles),
      onClick: openFiles
    }, ariaLabels.unsupportedFileType));
  } else if (dropHandlers.length > 0) {
    uploadButton = ___EmotionJSX(EuiButtonIcon, {
      size: "s",
      iconType: "paperClip",
      color: "text",
      "aria-label": ariaLabels.openUploadModal,
      onClick: openFiles
    });
  }

  var errorsButton;

  if (errors && errors.length) {
    errorsButton = ___EmotionJSX(EuiPopover, {
      button: ___EmotionJSX(EuiButtonEmpty, {
        iconType: "crossInACircleFilled",
        size: "s",
        color: "danger",
        "aria-label": ariaLabels.showSyntaxErrors,
        onClick: onButtonClick
      }, errors.length),
      isOpen: isPopoverOpen,
      closePopover: closePopover,
      panelPaddingSize: "s",
      anchorPosition: "upCenter"
    }, ___EmotionJSX("div", {
      className: "euiMarkdownEditorFooter__popover"
    }, ___EmotionJSX(EuiPopoverTitle, null, ___EmotionJSX(EuiI18n, {
      token: "euiMarkdownEditorFooter.errorsTitle",
      default: "Errors"
    })), errors.map(function (message, idx) {
      return ___EmotionJSX(EuiText, {
        size: "s",
        key: idx
      }, message.toString());
    })));
  }

  var uiPluginsWithHelpText = uiPlugins.filter(function (_ref2) {
    var helpText = _ref2.helpText;
    return !!helpText;
  });
  var hasUiPluginsWithHelpText = uiPluginsWithHelpText.length > 0;
  var mdSyntaxHref = 'https://guides.github.com/features/mastering-markdown/';

  var mdSyntaxLink = ___EmotionJSX(EuiLink, {
    href: mdSyntaxHref,
    target: "_blank"
  }, ___EmotionJSX(EuiI18n, {
    token: "euiMarkdownEditorFooter.mdSyntaxLink",
    default: "GitHub flavored markdown"
  }));

  var helpSyntaxButton;

  if (hasUiPluginsWithHelpText) {
    helpSyntaxButton = ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiToolTip, {
      content: syntaxTitle
    }, ___EmotionJSX(EuiButtonIcon, {
      size: "s",
      className: "euiMarkdownEditorFooter__helpButton",
      iconType: MarkdownLogo,
      color: "text",
      "aria-label": ariaLabels.showMarkdownHelp,
      onClick: function onClick() {
        return setIsShowingHelpModal(!isShowingHelpModal);
      }
    })), isShowingHelpModal && ___EmotionJSX(EuiModal, {
      onClose: function onClose() {
        return setIsShowingHelpModal(false);
      }
    }, ___EmotionJSX(EuiModalHeader, null, ___EmotionJSX(EuiTitle, null, ___EmotionJSX("h1", null, syntaxTitle))), ___EmotionJSX(EuiModalBody, null, ___EmotionJSX(EuiText, null, ___EmotionJSX(EuiI18n, {
      tokens: ['euiMarkdownEditorFooter.syntaxModalDescriptionPrefix', 'euiMarkdownEditorFooter.syntaxModalDescriptionSuffix'],
      defaults: ['This editor uses', 'You can also utilize these additional syntax plugins to add rich content to your text.']
    }, function (_ref3) {
      var _ref4 = _slicedToArray(_ref3, 2),
          syntaxModalDescriptionPrefix = _ref4[0],
          syntaxModalDescriptionSuffix = _ref4[1];

      return ___EmotionJSX("p", null, syntaxModalDescriptionPrefix, " ", mdSyntaxLink, ".", ' ', syntaxModalDescriptionSuffix);
    })), ___EmotionJSX(EuiHorizontalRule, null), uiPluginsWithHelpText.map(function (_ref5) {
      var name = _ref5.name,
          helpText = _ref5.helpText;
      return ___EmotionJSX(Fragment, {
        key: name
      }, ___EmotionJSX(EuiTitle, {
        size: "xxs"
      }, ___EmotionJSX("p", null, ___EmotionJSX("strong", null, name))), ___EmotionJSX(EuiSpacer, {
        size: "s"
      }), helpText, ___EmotionJSX(EuiSpacer, {
        size: "l"
      }));
    }), ___EmotionJSX(EuiHorizontalRule, null)), ___EmotionJSX(EuiModalFooter, null, ___EmotionJSX(EuiButton, {
      onClick: function onClick() {
        return setIsShowingHelpModal(false);
      },
      fill: true
    }, ___EmotionJSX(EuiI18n, {
      token: "euiMarkdownEditorFooter.closeButton",
      default: "Close"
    })))));
  } else {
    helpSyntaxButton = ___EmotionJSX(EuiPopover, {
      button: ___EmotionJSX(EuiButtonIcon, {
        title: syntaxTitle,
        size: "s",
        className: "euiMarkdownEditorFooter__helpButton",
        iconType: MarkdownLogo,
        color: "text",
        "aria-label": ariaLabels.showMarkdownHelp,
        onClick: function onClick() {
          return setIsShowingHelpPopover(!isShowingHelpPopover);
        }
      }),
      isOpen: isShowingHelpPopover,
      closePopover: function closePopover() {
        return setIsShowingHelpPopover(false);
      },
      panelPaddingSize: "s",
      anchorPosition: "upCenter"
    }, ___EmotionJSX(EuiI18n, {
      tokens: ['euiMarkdownEditorFooter.syntaxPopoverDescription'],
      defaults: ['This editor uses']
    }, function (_ref6) {
      var _ref7 = _slicedToArray(_ref6, 1),
          syntaxPopoverDescription = _ref7[0];

      return ___EmotionJSX("p", null, syntaxPopoverDescription, " ", mdSyntaxLink, ".");
    }));
  }

  return ___EmotionJSX("div", {
    ref: ref,
    className: "euiMarkdownEditorFooter"
  }, ___EmotionJSX("div", {
    className: "euiMarkdownEditorFooter__actions"
  }, uploadButton, errorsButton), helpSyntaxButton);
});
EuiMarkdownEditorFooter.propTypes = {
  uiPlugins: PropTypes.arrayOf(PropTypes.shape({
    name: PropTypes.string.isRequired,
    button: PropTypes.shape({
      label: PropTypes.string.isRequired,
      iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]).isRequired
    }).isRequired,
    helpText: PropTypes.node,
    formatting: PropTypes.shape({
      prefix: PropTypes.string,
      suffix: PropTypes.string,
      blockPrefix: PropTypes.string,
      blockSuffix: PropTypes.string,
      multiline: PropTypes.bool,
      replaceNext: PropTypes.string,
      prefixSpace: PropTypes.bool,
      scanFor: PropTypes.string,
      surroundWithNewlines: PropTypes.bool,
      orderedList: PropTypes.bool,
      trimFirst: PropTypes.bool
    }),
    editor: PropTypes.elementType
  }).isRequired).isRequired,
  isUploadingFiles: PropTypes.bool.isRequired,
  openFiles: PropTypes.func.isRequired,
  errors: PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.any.isRequired, PropTypes.any.isRequired]).isRequired).isRequired,
  hasUnacceptedItems: PropTypes.bool.isRequired,
  dropHandlers: PropTypes.arrayOf(PropTypes.shape({
    supportedFiles: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    accepts: PropTypes.func.isRequired,
    getFormattingForItem: PropTypes.func.isRequired
  }).isRequired).isRequired
};
EuiMarkdownEditorFooter.displayName = 'EuiMarkdownEditorFooter';