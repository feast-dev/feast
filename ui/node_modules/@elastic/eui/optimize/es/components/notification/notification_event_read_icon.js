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
import { useEuiI18n } from '../i18n';
import classNames from 'classnames';
import { EuiIcon } from '../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiNotificationEventReadIcon = function EuiNotificationEventReadIcon(_ref) {
  var id = _ref.id,
      isRead = _ref.isRead,
      eventName = _ref.eventName,
      rest = _objectWithoutProperties(_ref, ["id", "isRead", "eventName"]);

  var classesReadState = classNames('euiNotificationEventReadIcon', {
    'euiNotificationEventReadIcon--isRead': isRead
  });
  var readAria = useEuiI18n('euiNotificationEventReadIcon.readAria', '{eventName} is read', {
    eventName: eventName
  });
  var unreadAria = useEuiI18n('euiNotificationEventReadIcon.unreadAria', '{eventName} is unread', {
    eventName: eventName
  });
  var readTitle = useEuiI18n('euiNotificationEventReadIcon.read', 'Read');
  var unreadTitle = useEuiI18n('euiNotificationEventReadIcon.unread', 'Unread');
  var iconAriaLabel = isRead ? readAria : unreadAria;
  var iconTitle = isRead ? readTitle : unreadTitle;
  return ___EmotionJSX("div", {
    className: classesReadState
  }, ___EmotionJSX(EuiIcon, _extends({
    type: "dot",
    "aria-label": iconAriaLabel,
    title: iconTitle,
    color: "primary",
    "data-test-subj": "".concat(id, "-notificationEventReadIcon")
  }, rest)));
};