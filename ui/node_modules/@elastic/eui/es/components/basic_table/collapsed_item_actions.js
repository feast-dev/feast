function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function"); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, writable: true, configurable: true } }); if (superClass) _setPrototypeOf(subClass, superClass); }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _possibleConstructorReturn(self, call) { if (call && (_typeof(call) === "object" || typeof call === "function")) { return call; } return _assertThisInitialized(self); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Component } from 'react';
import PropTypes from "prop-types";
import { isString } from '../../services/predicate';
import { EuiContextMenuItem, EuiContextMenuPanel } from '../context_menu';
import { EuiPopover } from '../popover';
import { EuiButtonIcon } from '../button';
import { EuiToolTip } from '../tool_tip';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";

function actionIsCustomItemAction(action) {
  return action.hasOwnProperty('render');
}

export var CollapsedItemActions = /*#__PURE__*/function (_Component) {
  _inherits(CollapsedItemActions, _Component);

  var _super = _createSuper(CollapsedItemActions);

  function CollapsedItemActions() {
    var _this;

    _classCallCheck(this, CollapsedItemActions);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "popoverDiv", null);

    _defineProperty(_assertThisInitialized(_this), "state", {
      popoverOpen: false
    });

    _defineProperty(_assertThisInitialized(_this), "togglePopover", function () {
      _this.setState(function (prevState) {
        return {
          popoverOpen: !prevState.popoverOpen
        };
      });
    });

    _defineProperty(_assertThisInitialized(_this), "closePopover", function () {
      _this.setState({
        popoverOpen: false
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onPopoverBlur", function () {
      // you must be asking... WTF? I know... but this timeout is
      // required to make sure we process the onBlur events after the initial
      // event cycle. Reference:
      // https://medium.com/@jessebeach/dealing-with-focus-and-blur-in-a-composite-widget-in-react-90d3c3b49a9b
      window.requestAnimationFrame(function () {
        if (!_this.popoverDiv.contains(document.activeElement) && _this.props.onBlur) {
          _this.props.onBlur();
        }
      });
    });

    _defineProperty(_assertThisInitialized(_this), "registerPopoverDiv", function (popoverDiv) {
      if (!_this.popoverDiv) {
        _this.popoverDiv = popoverDiv;

        _this.popoverDiv.addEventListener('focusout', _this.onPopoverBlur);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onClickItem", function (onClickAction) {
      _this.closePopover();

      if (onClickAction) {
        onClickAction();
      }
    });

    return _this;
  }

  _createClass(CollapsedItemActions, [{
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      if (this.popoverDiv) {
        this.popoverDiv.removeEventListener('focusout', this.onPopoverBlur);
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props = this.props,
          actions = _this$props.actions,
          itemId = _this$props.itemId,
          item = _this$props.item,
          actionEnabled = _this$props.actionEnabled,
          onFocus = _this$props.onFocus,
          className = _this$props.className;
      var isOpen = this.state.popoverOpen;
      var allDisabled = true;
      var controls = actions.reduce(function (controls, action, index) {
        var key = "action_".concat(itemId, "_").concat(index);
        var available = action.available ? action.available(item) : true;

        if (!available) {
          return controls;
        }

        var enabled = actionEnabled(action);
        allDisabled = allDisabled && !enabled;

        if (actionIsCustomItemAction(action)) {
          var customAction = action;
          var actionControl = customAction.render(item, enabled);
          var actionControlOnClick = actionControl && actionControl.props && actionControl.props.onClick;
          controls.push(___EmotionJSX(EuiContextMenuItem, {
            key: key,
            onClick: function onClick() {
              return _this2.onClickItem(actionControlOnClick ? function () {
                return actionControlOnClick(item);
              } : undefined);
            }
          }, actionControl));
        } else {
          var _onClick = action.onClick,
              name = action.name,
              href = action.href,
              target = action.target,
              dataTestSubj = action['data-test-subj'];
          var buttonIcon = action.icon;
          var icon;

          if (buttonIcon) {
            icon = isString(buttonIcon) ? buttonIcon : buttonIcon(item);
          }

          var buttonContent = typeof name === 'function' ? name(item) : name;
          controls.push(___EmotionJSX(EuiContextMenuItem, {
            key: key,
            disabled: !enabled,
            href: href,
            target: target,
            icon: icon,
            "data-test-subj": dataTestSubj,
            onClick: function onClick() {
              return _this2.onClickItem(_onClick ? function () {
                return _onClick(item);
              } : undefined);
            }
          }, buttonContent));
        }

        return controls;
      }, []);

      var popoverButton = ___EmotionJSX(EuiI18n, {
        token: "euiCollapsedItemActions.allActions",
        default: "All actions"
      }, function (allActions) {
        return ___EmotionJSX(EuiButtonIcon, {
          className: className,
          "aria-label": allActions,
          iconType: "boxesHorizontal",
          color: "text",
          isDisabled: allDisabled,
          onClick: _this2.togglePopover.bind(_this2),
          onFocus: onFocus,
          "data-test-subj": "euiCollapsedItemActionsButton"
        });
      });

      var withTooltip = !allDisabled && ___EmotionJSX(EuiI18n, {
        token: "euiCollapsedItemActions.allActions",
        default: "All actions"
      }, function (allActions) {
        return ___EmotionJSX(EuiToolTip, {
          content: allActions,
          delay: "long"
        }, popoverButton);
      });

      return ___EmotionJSX(EuiPopover, {
        className: className,
        popoverRef: this.registerPopoverDiv,
        id: "".concat(itemId, "-actions"),
        isOpen: isOpen,
        button: withTooltip || popoverButton,
        closePopover: this.closePopover,
        panelPaddingSize: "none",
        anchorPosition: "leftCenter"
      }, ___EmotionJSX(EuiContextMenuPanel, {
        items: controls
      }));
    }
  }]);

  return CollapsedItemActions;
}(Component);
CollapsedItemActions.propTypes = {
  actions: PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.shape({
    /**
       * The type of action
       */
    type: PropTypes.oneOfType([PropTypes.oneOf(["button"]), PropTypes.oneOf(["icon"]).isRequired]),

    /**
       * Defines the color of the button
       */
    color: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.oneOf(["primary", "danger", "text", "ghost", "success", "warning"]).isRequired, PropTypes.func.isRequired]), PropTypes.oneOfType([PropTypes.oneOf(["primary", "accent", "success", "warning", "danger", "ghost", "text"]).isRequired, PropTypes.func.isRequired])]),

    /**
       * The display name of the action (will be the button caption)
       */

    /**
       * The display name of the action (will be the button caption)
       */
    name: PropTypes.oneOfType([PropTypes.node.isRequired, PropTypes.func.isRequired]).isRequired,

    /**
       * Describes the action (will be the button title)
       */

    /**
       * Describes the action (will be the button title)
       */
    description: PropTypes.string.isRequired,

    /**
       * A handler function to execute the action
       */

    /**
       * A handler function to execute the action
       */
    onClick: PropTypes.func,
    href: PropTypes.string,
    target: PropTypes.string,

    /**
       * A callback function that determines whether the action is available
       */

    /**
       * A callback function that determines whether the action is available
       */
    available: PropTypes.func,

    /**
       * A callback function that determines whether the action is enabled
       */

    /**
       * A callback function that determines whether the action is enabled
       */
    enabled: PropTypes.func,
    isPrimary: PropTypes.bool,
    "data-test-subj": PropTypes.string,

    /**
       * Associates an icon with the button
       */
    icon: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.func.isRequired])
  }).isRequired, PropTypes.shape({
    /**
       * The function that renders the action. Note that the returned node is expected to have `onFocus` and `onBlur` functions
       */
    render: PropTypes.func.isRequired,

    /**
       * A callback that defines whether the action is available
       */
    available: PropTypes.func,

    /**
       * A callback that defines whether the action is enabled
       */
    enabled: PropTypes.func,
    isPrimary: PropTypes.bool
  }).isRequired]).isRequired).isRequired,
  item: PropTypes.any.isRequired,
  itemId: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired]).isRequired,
  actionEnabled: PropTypes.func.isRequired,
  className: PropTypes.string,
  onFocus: PropTypes.func,
  onBlur: PropTypes.func
};