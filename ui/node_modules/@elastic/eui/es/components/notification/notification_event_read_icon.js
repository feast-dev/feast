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
EuiNotificationEventReadIcon.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

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
  onIconLoad: PropTypes.func,
  id: PropTypes.string.isRequired,

  /**
     * Shows an indicator of the read state of the event
     */
  isRead: PropTypes.bool.isRequired,

  /**
     * A unique, human-friendly name for the event to be used in aria attributes (e.g. "alert-critical-01", "cloud-no-severity-12", etc..).
     */
  eventName: PropTypes.string.isRequired
};