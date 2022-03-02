import { QueryKey } from '../core/types';
import { QueryFilters } from '../core/utils';
export declare function useIsFetching(filters?: QueryFilters): number;
export declare function useIsFetching(queryKey?: QueryKey, filters?: QueryFilters): number;
