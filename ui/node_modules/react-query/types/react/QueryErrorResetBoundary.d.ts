import React from 'react';
interface QueryErrorResetBoundaryValue {
    clearReset: () => void;
    isReset: () => boolean;
    reset: () => void;
}
export declare const useQueryErrorResetBoundary: () => QueryErrorResetBoundaryValue;
export interface QueryErrorResetBoundaryProps {
    children: ((value: QueryErrorResetBoundaryValue) => React.ReactNode) | React.ReactNode;
}
export declare const QueryErrorResetBoundary: React.FC<QueryErrorResetBoundaryProps>;
export {};
