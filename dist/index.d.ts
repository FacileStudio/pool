export interface PoolConfig {
    app: string;
    instance: string;
    secret: string;
    instance_id?: string;
    events?: {
        emit?: string[];
        listen?: string[];
    };
}
export interface PoolManifest {
    app: string;
    instance?: string;
    secret?: string;
    instance_id?: string;
    events?: {
        emit?: string[];
        listen?: string[];
    };
}
export interface PoolClientOptions {
    config?: PoolConfig;
    configPath?: string;
    onConnect?: () => void;
    onDisconnect?: () => void;
    onError?: (error: Error) => void;
    reconnect?: boolean;
    maxReconnectAttempts?: number;
}
export interface EventMeta {
    id: string;
    channel: string;
    offset: number;
    timestamp: number;
    sender: string;
}
type EventHandler<T = unknown> = (payload: T, meta: EventMeta) => void | Promise<void>;
type Unsubscribe = () => void;
export declare function loadConfig(path?: string): PoolConfig;
export declare function loadManifest(path?: string): PoolManifest;
export declare class PoolClient {
    private config;
    private ws;
    private handlers;
    private offsets;
    private epoch;
    private appId;
    private token;
    private connected;
    private reconnecting;
    private reconnectAttempt;
    private shouldReconnect;
    private maxReconnectAttempts;
    private _onConnect?;
    private _onDisconnect?;
    private _onError?;
    private pendingMessages;
    private reconnectTimer;
    constructor(options: PoolClientOptions);
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    emit<T = unknown>(channel: string, payload: T): void;
    listen<T = unknown>(channel: string, handler: EventHandler<T>): Unsubscribe;
    get isConnected(): boolean;
    identity(): string;
    get currentEpoch(): string;
    private register;
    private openWebSocket;
    private handleMessage;
    private scheduleReconnect;
}
export {};
