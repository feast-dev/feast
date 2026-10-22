import React from "react";
import { EuiBadge, EuiFlexGroup, EuiFlexItem } from "@elastic/eui";

interface SearchTokensListProps {
  tokens: string[];
  removeTokenByPosition: (tokenPosition: number) => void;
}

const SearchTokensList = ({
  tokens,
  removeTokenByPosition,
}: SearchTokensListProps) => {
  return (
    <EuiFlexGroup wrap responsive={false} gutterSize="xs">
      {tokens.map((token, index) => {
        const badgeColor = token.indexOf(":") > 0 ? "primary" : "hollow";

        return (
          <EuiFlexItem key={token} grow={false}>
            <EuiBadge
              color={badgeColor}
              iconType="cross"
              iconSide="right"
              iconOnClick={() => {
                removeTokenByPosition(index);
              }}
              iconOnClickAriaLabel="Example of onClick event for icon within the button"
            >
              {token}
            </EuiBadge>
          </EuiFlexItem>
        );
      })}
    </EuiFlexGroup>
  );
};

export default SearchTokensList;
