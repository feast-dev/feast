import * as React from "react";
import { Location } from "history";
export interface StaticRouterProps {
    basename?: string;
    children?: React.ReactNode;
    location: Partial<Location> | string;
}
/**
 * A <Router> that may not transition to any other location. This is useful
 * on the server where there is no stateful UI.
 */
export declare function StaticRouter({ basename, children, location: locationProp }: StaticRouterProps): JSX.Element;
