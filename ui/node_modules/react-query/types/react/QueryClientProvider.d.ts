import React from 'react';
import { QueryClient } from '../core';
declare global {
    interface Window {
        ReactQueryClientContext?: React.Context<QueryClient | undefined>;
    }
}
export declare const useQueryClient: () => QueryClient;
export interface QueryClientProviderProps {
    client: QueryClient;
    contextSharing?: boolean;
}
export declare const QueryClientProvider: React.FC<QueryClientProviderProps>;
