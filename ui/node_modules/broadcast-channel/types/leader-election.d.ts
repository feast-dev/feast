import {
    BroadcastChannel,
    OnMessageHandler
} from './broadcast-channel';

export type LeaderElectionOptions = {
    /**
     * This value decides how often instances will renegotiate who is leader.
     * Probably should be at least 2x bigger than responseTime.
     */
    fallbackInterval?: number;
    /**
     * This timer value is used when resolving which instance should be leader.
     * In case when your application elects more than one leader increase this value.
     */
    responseTime?: number;
};

export declare class LeaderElector {

    /**
     * IMPORTANT: The leader election is lazy,
     * it will not start before you call awaitLeadership()
     * so isLeader will never become true then
     */
    readonly isLeader: boolean;

    readonly isDead: boolean;
    readonly token: string;

    applyOnce(): Promise<boolean>;
    awaitLeadership(): Promise<void>;
    die(): Promise<void>;

    /**
     * Add an event handler that is run
     * when it is detected that there are duplicate leaders
     */
    onduplicate: OnMessageHandler<any>;
}

type CreateFunction = (channel: BroadcastChannel, options?: LeaderElectionOptions) => LeaderElector;

export const createLeaderElection: CreateFunction;
