import React from 'react';
import { HydrateOptions } from '../core';
export declare function useHydrate(state: unknown, options?: HydrateOptions): void;
export interface HydrateProps {
    state?: unknown;
    options?: HydrateOptions;
}
export declare const Hydrate: React.FC<HydrateProps>;
