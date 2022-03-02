import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useState, useMemo, Fragment, forwardRef } from 'react';
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
EuiMarkdownEditorFooter.displayName = 'EuiMarkdownEditorFooter';