import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { EuiButtonIcon } from '../button';
import { useEuiI18n } from '../i18n';
import classNames from 'classnames';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiNotificationEventReadButton = function EuiNotificationEventReadButton(_ref) {
  var id = _ref.id,
      isRead = _ref.isRead,
      onClick = _ref.onClick,
      eventName = _ref.eventName,
      rest = _objectWithoutProperties(_ref, ["id", "isRead", "onClick", "eventName"]);

  var classesReadState = classNames('euiNotificationEventReadButton', {
    'euiNotificationEventReadButton--isRead': isRead
  });
  var markAsReadAria = useEuiI18n('euiNotificationEventReadButton.markAsReadAria', 'Mark {eventName} as read', {
    eventName: eventName
  });
  var markAsUnreadAria = useEuiI18n('euiNotificationEventReadButton.markAsUnreadAria', 'Mark {eventName} as unread', {
    eventName: eventName
  });
  var markAsRead = useEuiI18n('euiNotificationEventReadButton.markAsRead', 'Mark as read');
  var markAsUnread = useEuiI18n('euiNotificationEventReadButton.markAsUnread', 'Mark as unread');
  var buttonAriaLabel = isRead ? markAsUnreadAria : markAsReadAria;
  var buttonTitle = isRead ? markAsUnread : markAsRead;
  return ___EmotionJSX(EuiButtonIcon, _extends({
    iconType: "dot",
    "aria-label": buttonAriaLabel,
    title: buttonTitle,
    className: classesReadState,
    onClick: onClick,
    "data-test-subj": "".concat(id, "-notificationEventReadButton")
  }, rest));
};