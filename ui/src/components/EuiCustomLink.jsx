// File name: "EuiCustomLink.js".
import React from "react";
import { EuiLink } from "@elastic/eui";
import { useNavigate, useHref } from "react-router-dom";

const isModifiedEvent = (event) =>
  !!(event.metaKey || event.altKey || event.ctrlKey || event.shiftKey);

const isLeftClickEvent = (event) => event.button === 0;

const isTargetBlank = (event) => {
  const target = event.target.getAttribute("target");
  return target && target !== "_self";
};

export default function EuiCustomLink({ to, ...rest }) {
  // This is the key!
  const navigate = useNavigate();

  function onClick(event) {
    if (event.defaultPrevented) {
      return;
    }

    // Let the browser handle links that open new tabs/windows
    if (
      isModifiedEvent(event) ||
      !isLeftClickEvent(event) ||
      isTargetBlank(event)
    ) {
      return;
    }

    // Prevent regular link behavior, which causes a browser refresh.
    event.preventDefault();

    // Push the route to the history.
    navigate(to);
  }

  // Generate the correct link href (with basename accounted for)
  const href = useHref({ pathname: to });

  const props = { ...rest, href, onClick };
  return <EuiLink {...props} />;
}
