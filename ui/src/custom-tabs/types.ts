import {
  useLoadOnDemandFeatureView,
  useLoadStreamFeatureView,
  useLoadRegularFeatureView,
} from "../pages/feature-views/useLoadFeatureView";
import useLoadFeature from "../pages/features/useLoadFeature";
import useLoadFeatureService from "../pages/feature-services/useLoadFeatureService";
import useLoadDataSource from "../pages/data-sources/useLoadDataSource";
import useLoadEntity from "../pages/entities/useLoadEntity";
import useLoadDataset from "../pages/saved-data-sets/useLoadDataset";

interface CustomTabRegistrationInterface {
  label: string;
  path: string;
  Component: (...args: any[]) => JSX.Element;
}

// Type for Regular Feature View Custom Tabs
type RegularFeatureViewQueryReturnType = ReturnType<
  typeof useLoadRegularFeatureView
>;
interface RegularFeatureViewCustomTabProps {
  id: string | undefined;
  feastObjectQuery: RegularFeatureViewQueryReturnType;
}
interface RegularFeatureViewCustomTabRegistrationInterface
  extends CustomTabRegistrationInterface {
  Component: ({
    id,
    feastObjectQuery,
    ...args
  }: RegularFeatureViewCustomTabProps) => JSX.Element;
}

// Type for OnDemand Feature View Custom Tabs
type OnDemandFeatureViewQueryReturnType = ReturnType<
  typeof useLoadOnDemandFeatureView
>;
interface OnDemandFeatureViewCustomTabProps {
  id: string | undefined;
  feastObjectQuery: OnDemandFeatureViewQueryReturnType;
}
interface OnDemandFeatureViewCustomTabRegistrationInterface
  extends CustomTabRegistrationInterface {
  Component: ({
    id,
    feastObjectQuery,
    ...args
  }: OnDemandFeatureViewCustomTabProps) => JSX.Element;
}

// Type for Stream Feature View Custom Tabs
type StreamFeatureViewQueryReturnType = ReturnType<
  typeof useLoadStreamFeatureView
>;
interface StreamFeatureViewCustomTabProps {
  id: string | undefined;
  feastObjectQuery: StreamFeatureViewQueryReturnType;
}
interface StreamFeatureViewCustomTabRegistrationInterface
  extends CustomTabRegistrationInterface {
  Component: ({
    id,
    feastObjectQuery,
    ...args
  }: StreamFeatureViewCustomTabProps) => JSX.Element;
}

// Type for Entity Custom Tabs
interface EntityCustomTabProps {
  id: string | undefined;
  feastObjectQuery: ReturnType<typeof useLoadEntity>;
}
interface EntityCustomTabRegistrationInterface
  extends CustomTabRegistrationInterface {
  Component: ({
    id,
    feastObjectQuery,
    ...args
  }: EntityCustomTabProps) => JSX.Element;
}

// Type for Feature Custom Tabs
interface FeatureCustomTabProps {
  id: string | undefined;
  feastObjectQuery: ReturnType<typeof useLoadFeature>;
}
interface FeatureCustomTabRegistrationInterface
  extends CustomTabRegistrationInterface {
  Component: ({
    id,
    feastObjectQuery,
    ...args
  }: FeatureCustomTabProps) => JSX.Element;
}

// Type for Feature Service Custom Tabs
interface FeatureServiceCustomTabProps {
  id: string | undefined;
  feastObjectQuery: ReturnType<typeof useLoadFeatureService>;
}
interface FeatureServiceCustomTabRegistrationInterface
  extends CustomTabRegistrationInterface {
  Component: ({
    id,
    feastObjectQuery,
    ...args
  }: FeatureServiceCustomTabProps) => JSX.Element;
}

// Type for Data Source Custom Tabs
interface DataSourceCustomTabProps {
  id: string | undefined;
  feastObjectQuery: ReturnType<typeof useLoadDataSource>;
}
interface DataSourceCustomTabRegistrationInterface
  extends CustomTabRegistrationInterface {
  Component: ({
    id,
    feastObjectQuery,
    ...args
  }: DataSourceCustomTabProps) => JSX.Element;
}

// Type for Data Source Custom Tabs
interface DatasetCustomTabProps {
  id: string | undefined;
  feastObjectQuery: ReturnType<typeof useLoadDataset>;
}
interface DatasetCustomTabRegistrationInterface
  extends CustomTabRegistrationInterface {
  Component: ({
    id,
    feastObjectQuery,
    ...args
  }: DatasetCustomTabProps) => JSX.Element;
}

// Type for Data Labeling Custom Tabs
interface DataLabelingCustomTabProps {
  id: string | undefined;
  feastObjectQuery: RegularFeatureViewQueryReturnType;
}
interface DataLabelingCustomTabRegistrationInterface
  extends CustomTabRegistrationInterface {
  Component: ({
    id,
    feastObjectQuery,
    ...args
  }: DataLabelingCustomTabProps) => JSX.Element;
}

export type {
  CustomTabRegistrationInterface,
  RegularFeatureViewQueryReturnType,
  RegularFeatureViewCustomTabRegistrationInterface,
  RegularFeatureViewCustomTabProps,
  OnDemandFeatureViewQueryReturnType,
  OnDemandFeatureViewCustomTabProps,
  OnDemandFeatureViewCustomTabRegistrationInterface,
  StreamFeatureViewQueryReturnType,
  StreamFeatureViewCustomTabProps,
  StreamFeatureViewCustomTabRegistrationInterface,
  FeatureServiceCustomTabRegistrationInterface,
  FeatureServiceCustomTabProps,
  DataSourceCustomTabRegistrationInterface,
  DataSourceCustomTabProps,
  EntityCustomTabRegistrationInterface,
  EntityCustomTabProps,
  FeatureCustomTabRegistrationInterface,
  FeatureCustomTabProps,
  DatasetCustomTabRegistrationInterface,
  DatasetCustomTabProps,
  DataLabelingCustomTabRegistrationInterface,
  DataLabelingCustomTabProps,
};
