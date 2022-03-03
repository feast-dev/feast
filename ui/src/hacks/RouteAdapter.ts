import React from "react";
import { Location } from "history";
import {
  useLocation,
  useNavigate,
  Location as RouterLocation,
} from "react-router-dom";

// via: https://github.com/pbeshai/use-query-params/issues/196#issuecomment-996893750
interface RouteAdapterProps {
  children: React.FunctionComponent<{
    history: {
      replace(location: Location): void;
      push(location: Location): void;
    };
    location: RouterLocation;
  }>;
}

// Via: https://github.com/pbeshai/use-query-params/blob/cd44e7fb3394620f757bfb09ff57b7f296d9a5e6/examples/react-router-6/src/index.js#L36
const RouteAdapter = ({ children }: RouteAdapterProps) => {
  const navigate = useNavigate();
  const location = useLocation();

  const adaptedHistory = React.useMemo(
    () => ({
      replace(location: Location) {
        navigate(location, { replace: true, state: location.state });
      },
      push(location: Location) {
        navigate(location, { replace: false, state: location.state });
      },
    }),
    [navigate]
  );
  return children && children({ history: adaptedHistory, location });
};

export default RouteAdapter;
