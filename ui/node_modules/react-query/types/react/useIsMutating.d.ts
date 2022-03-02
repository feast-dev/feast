import { MutationKey } from '../core/types';
import { MutationFilters } from '../core/utils';
export declare function useIsMutating(filters?: MutationFilters): number;
export declare function useIsMutating(mutationKey?: MutationKey, filters?: Omit<MutationFilters, 'mutationKey'>): number;
