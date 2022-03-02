import _extends from "@babel/runtime/helpers/extends";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { createElement } from 'react';
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