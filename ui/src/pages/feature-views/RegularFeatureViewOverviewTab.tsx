import {
  EuiBadge,
  EuiButtonEmpty,
  EuiCallOut,
  EuiFlexGroup,
  EuiFlexItem,
  EuiHorizontalRule,
  EuiPanel,
  EuiSpacer,
  EuiStat,
  EuiText,
  EuiTitle,
  EuiToolTip,
} from "@elastic/eui";
import React, { useState } from "react";

import { useNavigate, useParams } from "react-router-dom";
import FeaturesListDisplay from "../../components/FeaturesListDisplay";
import FeatureViewFormModal, {
  FeatureViewFormData,
} from "../../components/FeatureViewFormModal";
import PermissionsDisplay from "../../components/PermissionsDisplay";
import TagsDisplay from "../../components/TagsDisplay";
import { encodeSearchQueryString } from "../../hooks/encodeSearchQueryString";
import { EntityRelation } from "../../parsers/parseEntityRelationships";
import { FEAST_FCO_TYPES } from "../../parsers/types";
import useLoadRelationshipData from "../../queries/useLoadRelationshipsData";
import useLoadFeatureUsage from "../../queries/useLoadFeatureUsage";
import { useApplyFeatureView } from "../../queries/mutations/useFeatureViewMutations";
import { getEntityPermissions } from "../../utils/permissionUtils";
import BatchSourcePropertiesView from "../data-sources/BatchSourcePropertiesView";
import ConsumingFeatureServicesList from "./ConsumingFeatureServicesList";
import FeatureViewUsagePanel from "./FeatureViewUsagePanel";
import { feast } from "../../protos";
import { toDate } from "../../utils/timestamp";

const whereFSconsumesThisFv = (fvName: string) => {
  return (r: EntityRelation) => {
    return (
      r.source.name === fvName &&
      r.target.type === FEAST_FCO_TYPES.featureService
    );
  };
};

interface RegularFeatureViewOverviewTabProps {
  data: feast.core.IFeatureView;
  permissions?: any[];
}

const buildEditFormData = (
  fv: feast.core.IFeatureView,
): FeatureViewFormData => {
  const tags = fv.spec?.tags
    ? Object.entries(fv.spec.tags).map(([key, value]) => ({ key, value }))
    : [];

  const features = (fv.spec?.features || []).map((f) => ({
    name: f.name || "",
    valueType: String(f.valueType ?? 0),
    description: f.description || "",
  }));

  let ttlValue = 0;
  let ttlUnit = "seconds";
  if (fv.spec?.ttl?.seconds) {
    const secs =
      typeof fv.spec.ttl.seconds === "number"
        ? fv.spec.ttl.seconds
        : ((fv.spec.ttl.seconds as any).toNumber?.() ?? 0);
    if (secs > 0 && secs % 86400 === 0) {
      ttlValue = secs / 86400;
      ttlUnit = "days";
    } else if (secs > 0 && secs % 3600 === 0) {
      ttlValue = secs / 3600;
      ttlUnit = "hours";
    } else if (secs > 0 && secs % 60 === 0) {
      ttlValue = secs / 60;
      ttlUnit = "minutes";
    } else {
      ttlValue = secs;
      ttlUnit = "seconds";
    }
  }

  return {
    name: fv.spec?.name || "",
    description: fv.spec?.description || "",
    owner: fv.spec?.owner || "",
    entities: fv.spec?.entities || [],
    features,
    batchSource: fv.spec?.batchSource?.name || "",
    ttlValue,
    ttlUnit,
    online: fv.spec?.online ?? true,
    tags,
  };
};

const RegularFeatureViewOverviewTab = ({
  data,
  permissions,
}: RegularFeatureViewOverviewTabProps) => {
  const navigate = useNavigate();

  const { projectName } = useParams();
  const { featureViewName } = useParams();

  const fvName = featureViewName === undefined ? "" : featureViewName;

  const relationshipQuery = useLoadRelationshipData();
  const { data: usageData } = useLoadFeatureUsage();

  const fsNames = relationshipQuery.data
    ? relationshipQuery.data.filter(whereFSconsumesThisFv(fvName)).map((fs) => {
        return fs.target.name;
      })
    : [];
  const numOfFs = fsNames.length;

  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const applyFeatureView = useApplyFeatureView();

  const TTL_UNITS: Record<string, number> = {
    days: 86400,
    hours: 3600,
    minutes: 60,
    seconds: 1,
  };

  const handleEditSubmit = (formData: FeatureViewFormData) => {
    const payload = {
      name: formData.name,
      project: projectName || "",
      entities: formData.entities,
      features: formData.features.map((f) => ({
        name: f.name,
        value_type: parseInt(f.valueType, 10),
        description: f.description,
      })),
      batch_source: formData.batchSource,
      ttl_seconds: formData.ttlValue * (TTL_UNITS[formData.ttlUnit] || 1),
      online: formData.online,
      description: formData.description,
      owner: formData.owner,
      tags: Object.fromEntries(
        formData.tags.filter((t) => t.key.trim()).map((t) => [t.key, t.value]),
      ),
    };
    applyFeatureView.mutate(payload, {
      onSuccess: () => {
        setIsEditModalOpen(false);
        setErrorMessage(null);
        setSuccessMessage(
          `Feature view "${formData.name}" updated successfully.`,
        );
        setTimeout(() => setSuccessMessage(null), 5000);
      },
      onError: (err: unknown) => {
        const message =
          err instanceof Error ? err.message : "An unexpected error occurred.";
        setErrorMessage(message);
        setTimeout(() => setErrorMessage(null), 8000);
      },
    });
  };

  const fvUsage = usageData?.feature_usage?.[fvName];
  const runCount = fvUsage?.run_count ?? 0;
  const lastUsed = fvUsage?.last_used ?? null;
  const lastUsedLabel =
    lastUsed != null ? new Date(lastUsed).toLocaleDateString() : "N/A";

  return (
    <React.Fragment>
      {successMessage && (
        <>
          <EuiCallOut
            title={successMessage}
            color="success"
            iconType="check"
            size="s"
          />
          <EuiSpacer size="m" />
        </>
      )}
      {errorMessage && (
        <>
          <EuiCallOut
            title={errorMessage}
            color="danger"
            iconType="alert"
            size="s"
          />
          <EuiSpacer size="m" />
        </>
      )}
      <EuiFlexGroup justifyContent="flexEnd">
        <EuiFlexItem grow={false}>
          <EuiButtonEmpty
            iconType="pencil"
            onClick={() => setIsEditModalOpen(true)}
          >
            Edit Feature View
          </EuiButtonEmpty>
        </EuiFlexItem>
      </EuiFlexGroup>
      <EuiSpacer size="s" />
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiStat title={`${numOfFs}`} description="Consuming Services" />
        </EuiFlexItem>
        {usageData?.mlflow_enabled && (
          <>
            <EuiFlexItem>
              <EuiStat
                title={`${runCount}`}
                description="MLflow Training Runs"
              />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiToolTip
                position="top"
                content={
                  lastUsed != null
                    ? new Date(lastUsed).toLocaleString()
                    : "No usage recorded"
                }
              >
                <EuiStat
                  title={lastUsedLabel}
                  description="Last Used in MLflow"
                />
              </EuiToolTip>
            </EuiFlexItem>
          </>
        )}
      </EuiFlexGroup>
      <EuiSpacer size="l" />
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiPanel hasBorder={true}>
            <EuiTitle size="xs">
              <h3>Features ({data?.spec?.features?.length})</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {projectName && data?.spec?.features ? (
              <FeaturesListDisplay
                projectName={projectName}
                featureViewName={data?.spec?.name!}
                features={data.spec.features}
                link={true}
              />
            ) : (
              <EuiText>No features specified on this feature view.</EuiText>
            )}
          </EuiPanel>
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiPanel hasBorder={true} grow={false}>
            <EuiTitle size="xs">
              <h3>Entities</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {data?.spec?.entities ? (
              <EuiFlexGroup wrap responsive={false} gutterSize="xs">
                {data.spec.entities.map((entity) => {
                  return (
                    <EuiFlexItem grow={false} key={entity}>
                      <EuiBadge
                        color="primary"
                        onClick={() => {
                          navigate(`/p/${projectName}/entity/${entity}`);
                        }}
                        onClickAriaLabel={entity}
                        data-test-sub="testExample1"
                      >
                        {entity}
                      </EuiBadge>
                    </EuiFlexItem>
                  );
                })}
              </EuiFlexGroup>
            ) : (
              <EuiText>No Entities.</EuiText>
            )}
          </EuiPanel>
          <EuiSpacer size="m" />
          <EuiPanel hasBorder={true}>
            <EuiTitle size="xs">
              <h3>Consuming Feature Services</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {fsNames.length > 0 ? (
              <ConsumingFeatureServicesList fsNames={fsNames} />
            ) : (
              <EuiText>No services consume this feature view</EuiText>
            )}
          </EuiPanel>
          <EuiSpacer size="m" />
          {usageData?.mlflow_enabled && (
            <FeatureViewUsagePanel featureViewName={fvName} />
          )}
          <EuiSpacer size="m" />
          <EuiPanel hasBorder={true} grow={false}>
            <EuiTitle size="xs">
              <h3>Tags</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {data?.spec?.tags ? (
              <TagsDisplay
                tags={data.spec.tags}
                createLink={(key, value) => {
                  return (
                    `/p/${projectName}/feature-view?` +
                    encodeSearchQueryString(`${key}:${value}`)
                  );
                }}
                owner={data?.spec?.owner!}
                description={data?.spec?.description!}
              />
            ) : (
              <EuiText>No Tags specified on this feature view.</EuiText>
            )}
          </EuiPanel>
          <EuiSpacer size="m" />
          <EuiPanel hasBorder={true}>
            <EuiTitle size="xs">
              <h3>Permissions</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {permissions ? (
              <PermissionsDisplay
                permissions={getEntityPermissions(
                  permissions,
                  FEAST_FCO_TYPES.featureView,
                  data?.spec?.name,
                )}
              />
            ) : (
              <EuiText>No permissions defined for this feature view.</EuiText>
            )}
          </EuiPanel>
        </EuiFlexItem>
      </EuiFlexGroup>
      <EuiSpacer size="l" />
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiPanel hasBorder={true}>
            <EuiTitle size="xs">
              <h3>Batch Source</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            <BatchSourcePropertiesView batchSource={data?.spec?.batchSource!} />
          </EuiPanel>
        </EuiFlexItem>
      </EuiFlexGroup>
      <EuiSpacer size="l" />
      <EuiPanel>
        <EuiTitle size="xs">
          <h3>Materialization Intervals</h3>
        </EuiTitle>
        <React.Fragment>
          {data?.meta?.materializationIntervals?.map((interval, i) => {
            return (
              <p key={i}>
                {toDate(interval.startTime!).toLocaleDateString("en-CA")} to{" "}
                {toDate(interval.endTime!).toLocaleDateString("en-CA")}
              </p>
            );
          })}
        </React.Fragment>
      </EuiPanel>

      {isEditModalOpen && data && (
        <FeatureViewFormModal
          onClose={() => setIsEditModalOpen(false)}
          onSubmit={handleEditSubmit}
          initialData={buildEditFormData(data)}
          isEdit
        />
      )}
    </React.Fragment>
  );
};

export default RegularFeatureViewOverviewTab;
