import React from "react";
import { EuiLink, type EuiLinkAnchorProps } from "@elastic/eui";
import { useNavigate, useHref, type To } from "react-router-dom";

interface EuiCustomLinkProps extends Omit<EuiLinkAnchorProps, 'href'> {
  to: To;
}

const isModifiedEvent = (event: React.MouseEvent) =>
  !!(event.metaKey || event.altKey || event.ctrlKey || event.shiftKey);

const isLeftClickEvent = (event: React.MouseEvent) => event.button === 0;

const isTargetBlank = (event: React.MouseEvent) => {
  const target = (event.target as Element).getAttribute("target");
  return target && target !== "_self";
};

export default function EuiCustomLink({ to, ...rest }: EuiCustomLinkProps) {
  // This is the key!
  const navigate = useNavigate();

  const onClick: React.MouseEventHandler<HTMLAnchorElement> = (event) => {
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
  const href = useHref(to);

  return <EuiLink {...rest} href={href} onClick={onClick} />;
}
