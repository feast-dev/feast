import { QueryClient } from '../core';
interface BroadcastQueryClientOptions {
    queryClient: QueryClient;
    broadcastChannel?: string;
}
export declare function broadcastQueryClient({ queryClient, broadcastChannel, }: BroadcastQueryClientOptions): void;
export {};
