import React from "react";

import {
  EuiHorizontalRule,
  EuiPanel,
  EuiTitle,
  EuiBadge,
  EuiLoadingContent,
  EuiFlexGroup,
  EuiFlexItem,
  EuiSpacer,
} from "@elastic/eui";

import { useNavigate } from "react-router-dom";
import useFCOExploreSuggestions from "../hooks/useFCOExploreSuggestions";

const ExplorePanel = () => {
  const { isLoading, isSuccess, data } = useFCOExploreSuggestions();

  const navigate = useNavigate();

  return (
    <EuiPanel>
      <EuiTitle size="xs">
        <h3>Explore this Project</h3>
      </EuiTitle>
      <EuiHorizontalRule margin="xs" />
      {isLoading && <EuiLoadingContent lines={3} />}
      {isSuccess &&
        data &&
        data.map((suggestionGroup, i) => {
          return (
            <React.Fragment key={i}>
              <EuiTitle size="xxs">
                <h4>{suggestionGroup.title}</h4>
              </EuiTitle>
              <EuiFlexGroup wrap responsive={false} gutterSize="xs">
                {suggestionGroup.items.map((item, j) => {
                  return (
                    <EuiFlexItem grow={false} key={j}>
                      <EuiBadge
                        color="primary"
                        onClick={() => {
                          navigate(item.link);
                        }}
                        onClickAriaLabel={item.label}
                      >
                        {item.name} ({item.count})
                      </EuiBadge>
                    </EuiFlexItem>
                  );
                })}
              </EuiFlexGroup>
              <EuiSpacer size="s" />
            </React.Fragment>
          );
        })}
    </EuiPanel>
  );
};

export default ExplorePanel;
