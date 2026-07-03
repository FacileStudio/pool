import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { parse as parseYaml } from "yaml";
export function loadConfig(path) {
    const manifest = loadManifest(path);
    if (!manifest.instance || !manifest.secret) {
        throw new Error("nook.yaml: 'instance' and 'secret' are required for full config. Use loadManifest() for YAML without connection details.");
    }
    return manifest;
}
function findConfigFile() {
    const candidates = ["nook.yaml", "nook.yml"];
    const dir = process.cwd();
    for (const name of candidates) {
        const full = join(dir, name);
        if (existsSync(full))
            return full;
    }
    const envPath = process.env.NOOK_CONFIG_PATH;
    if (envPath && existsSync(envPath))
        return envPath;
    return null;
}
function validateConfig(config) {
    const c = config;
    if (!c.app || typeof c.app !== "string")
        throw new Error("nook.yaml: 'app' is required");
}
export function loadManifest(path) {
    const configPath = path || findConfigFile();
    if (!configPath) {
        throw new Error("No nook.yaml found. Provide a config path or inline config.");
    }
    const content = readFileSync(configPath, "utf-8");
    const parsed = parseYaml(content);
    validateConfig(parsed);
    return parsed;
}
export class PoolClient {
    constructor(options) {
        this.ws = null;
        this.handlers = new Map();
        this.offsets = new Map();
        this.epoch = "";
        this.appId = "";
        this.token = "";
        this.connected = false;
        this.reconnecting = false;
        this.reconnectAttempt = 0;
        this.pendingMessages = [];
        this.reconnectTimer = null;
        if (options.config) {
            this.config = options.config;
        }
        else {
            this.config = loadConfig(options.configPath);
        }
        this.shouldReconnect = options.reconnect !== false;
        this.maxReconnectAttempts = options.maxReconnectAttempts ?? 20;
        this._onConnect = options.onConnect;
        this._onDisconnect = options.onDisconnect;
        this._onError = options.onError;
    }
    async connect() {
        await this.register();
        await this.openWebSocket();
    }
    async disconnect() {
        this.shouldReconnect = false;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        if (this.ws) {
            this.ws.close(1000, "client disconnect");
            this.ws = null;
        }
        this.connected = false;
    }
    emit(channel, payload) {
        const msg = JSON.stringify({
            type: "event",
            id: crypto.randomUUID(),
            channel,
            payload,
            timestamp: Date.now(),
        });
        if (this.connected && this.ws) {
            this.ws.send(msg);
        }
        else {
            this.pendingMessages.push(msg);
        }
    }
    listen(channel, handler) {
        if (!this.handlers.has(channel)) {
            this.handlers.set(channel, new Set());
        }
        this.handlers.get(channel).add(handler);
        return () => {
            this.handlers.get(channel)?.delete(handler);
        };
    }
    get isConnected() {
        return this.connected;
    }
    identity() {
        return this.config.instance_id
            ? `${this.config.app}:${this.config.instance_id}`
            : this.config.app;
    }
    get currentEpoch() {
        return this.epoch;
    }
    async register() {
        const url = `${this.config.instance}/api/pool/register`;
        const res = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                app: this.config.app,
                instance_id: this.config.instance_id,
                secret: this.config.secret,
                events: this.config.events,
            }),
        });
        if (!res.ok) {
            const text = await res.text();
            throw new Error(`Pool registration failed: ${res.status} ${text}`);
        }
        const data = (await res.json());
        this.token = data.token;
        this.appId = data.app_id;
    }
    async openWebSocket() {
        return new Promise((resolve, reject) => {
            const wsUrl = this.config.instance.replace(/^http/, "ws") +
                `/api/pool/ws?token=${encodeURIComponent(this.token)}`;
            this.ws = new WebSocket(wsUrl);
            this.ws.onopen = () => {
                this.connected = true;
                this.reconnecting = false;
                this.reconnectAttempt = 0;
                const channels = {};
                const listenChannels = this.config.events?.listen ?? [];
                for (const ch of listenChannels) {
                    channels[ch] = { last_offset: this.offsets.get(ch) ?? 0 };
                }
                for (const ch of this.handlers.keys()) {
                    if (!channels[ch]) {
                        channels[ch] = { last_offset: this.offsets.get(ch) ?? 0 };
                    }
                }
                this.ws.send(JSON.stringify({ type: "subscribe", channels }));
                for (const msg of this.pendingMessages) {
                    this.ws.send(msg);
                }
                this.pendingMessages = [];
                this._onConnect?.();
                resolve();
            };
            this.ws.onmessage = (event) => {
                this.handleMessage(event.data);
            };
            this.ws.onclose = (event) => {
                const wasConnected = this.connected;
                this.connected = false;
                if (wasConnected) {
                    this._onDisconnect?.();
                }
                if (this.shouldReconnect && !event.wasClean) {
                    this.scheduleReconnect();
                }
            };
            this.ws.onerror = () => {
                const error = new Error("WebSocket connection error");
                this._onError?.(error);
                if (!this.connected) {
                    reject(error);
                }
            };
        });
    }
    handleMessage(raw) {
        let msg;
        try {
            msg = JSON.parse(raw);
        }
        catch {
            return;
        }
        switch (msg.type) {
            case "welcome": {
                this.epoch = msg.epoch;
                break;
            }
            case "event": {
                const sender = msg.sender;
                if (sender === this.identity())
                    return;
                const channel = msg.channel;
                const offset = msg.offset;
                this.offsets.set(channel, offset);
                const handlers = this.handlers.get(channel);
                if (handlers) {
                    const meta = {
                        id: msg.id,
                        channel,
                        offset,
                        timestamp: msg.timestamp,
                        sender,
                    };
                    for (const handler of handlers) {
                        try {
                            handler(msg.payload, meta);
                        }
                        catch (err) {
                            this._onError?.(err instanceof Error ? err : new Error(String(err)));
                        }
                    }
                }
                this.ws?.send(JSON.stringify({ type: "ack", channel, offset }));
                break;
            }
            case "error": {
                this._onError?.(new Error(`Pool error [${msg.code}]: ${msg.message}`));
                break;
            }
            case "pong":
                break;
        }
    }
    scheduleReconnect() {
        if (this.reconnecting)
            return;
        if (this.reconnectAttempt >= this.maxReconnectAttempts) {
            this._onError?.(new Error("Max reconnection attempts reached"));
            return;
        }
        this.reconnecting = true;
        const delay = Math.min(500 * Math.pow(2, this.reconnectAttempt), 30000) +
            Math.random() * 500;
        this.reconnectAttempt++;
        this.reconnectTimer = setTimeout(async () => {
            try {
                await this.register();
                await this.openWebSocket();
            }
            catch {
                this.reconnecting = false;
                this.scheduleReconnect();
            }
        }, delay);
    }
}
