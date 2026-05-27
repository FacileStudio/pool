import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { parse as parseYaml } from "yaml";

export interface PoolConfig {
  app: string;
  instance: string;
  secret: string;
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

type EventHandler<T = unknown> = (
  payload: T,
  meta: EventMeta,
) => void | Promise<void>;
type Unsubscribe = () => void;

export function loadConfig(path?: string): PoolConfig {
  const configPath = path || findConfigFile();
  if (!configPath) {
    throw new Error(
      "No nook.yaml found. Provide a config path or inline config.",
    );
  }
  const content = readFileSync(configPath, "utf-8");
  const parsed = parseYaml(content);
  validateConfig(parsed);
  return parsed as PoolConfig;
}

function findConfigFile(): string | null {
  const candidates = ["nook.yaml", "nook.yml"];
  const dir = process.cwd();

  for (const name of candidates) {
    const full = join(dir, name);
    if (existsSync(full)) return full;
  }

  const envPath = process.env.NOOK_CONFIG_PATH;
  if (envPath && existsSync(envPath)) return envPath;

  return null;
}

function validateConfig(config: unknown): asserts config is PoolConfig {
  const c = config as Record<string, unknown>;
  if (!c.app || typeof c.app !== "string")
    throw new Error("nook.yaml: 'app' is required");
  if (!c.instance || typeof c.instance !== "string")
    throw new Error("nook.yaml: 'instance' is required");
  if (!c.secret || typeof c.secret !== "string")
    throw new Error("nook.yaml: 'secret' is required");
}

export class PoolClient {
  private config: PoolConfig;
  private ws: WebSocket | null = null;
  private handlers: Map<string, Set<EventHandler>> = new Map();
  private offsets: Map<string, number> = new Map();
  private epoch = "";
  private appId = "";
  private token = "";
  private connected = false;
  private reconnecting = false;
  private reconnectAttempt = 0;
  private shouldReconnect: boolean;
  private maxReconnectAttempts: number;
  private _onConnect?: () => void;
  private _onDisconnect?: () => void;
  private _onError?: (error: Error) => void;
  private pendingMessages: string[] = [];
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(options: PoolClientOptions) {
    if (options.config) {
      this.config = options.config;
    } else {
      this.config = loadConfig(options.configPath);
    }
    this.shouldReconnect = options.reconnect !== false;
    this.maxReconnectAttempts = options.maxReconnectAttempts ?? 20;
    this._onConnect = options.onConnect;
    this._onDisconnect = options.onDisconnect;
    this._onError = options.onError;
  }

  async connect(): Promise<void> {
    await this.register();
    await this.openWebSocket();
  }

  async disconnect(): Promise<void> {
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

  emit<T = unknown>(channel: string, payload: T): void {
    const msg = JSON.stringify({
      type: "event",
      id: crypto.randomUUID(),
      channel,
      payload,
      timestamp: Date.now(),
    });

    if (this.connected && this.ws) {
      this.ws.send(msg);
    } else {
      this.pendingMessages.push(msg);
    }
  }

  listen<T = unknown>(channel: string, handler: EventHandler<T>): Unsubscribe {
    if (!this.handlers.has(channel)) {
      this.handlers.set(channel, new Set());
    }
    this.handlers.get(channel)!.add(handler as EventHandler);

    return () => {
      this.handlers.get(channel)?.delete(handler as EventHandler);
    };
  }

  get isConnected(): boolean {
    return this.connected;
  }

  get currentEpoch(): string {
    return this.epoch;
  }

  private async register(): Promise<void> {
    const url = `${this.config.instance}/api/pool/register`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        app: this.config.app,
        secret: this.config.secret,
        events: this.config.events,
      }),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Pool registration failed: ${res.status} ${text}`);
    }

    const data = (await res.json()) as { token: string; app_id: string };
    this.token = data.token;
    this.appId = data.app_id;
  }

  private async openWebSocket(): Promise<void> {
    return new Promise((resolve, reject) => {
      const wsUrl =
        this.config.instance.replace(/^http/, "ws") +
        `/api/pool/ws?token=${encodeURIComponent(this.token)}`;

      this.ws = new WebSocket(wsUrl);

      this.ws.onopen = () => {
        this.connected = true;
        this.reconnecting = false;
        this.reconnectAttempt = 0;

        const channels: Record<string, { last_offset: number }> = {};
        const listenChannels = this.config.events?.listen ?? [];
        for (const ch of listenChannels) {
          channels[ch] = { last_offset: this.offsets.get(ch) ?? 0 };
        }
        for (const ch of this.handlers.keys()) {
          if (!channels[ch]) {
            channels[ch] = { last_offset: this.offsets.get(ch) ?? 0 };
          }
        }

        this.ws!.send(JSON.stringify({ type: "subscribe", channels }));

        for (const msg of this.pendingMessages) {
          this.ws!.send(msg);
        }
        this.pendingMessages = [];

        this._onConnect?.();
        resolve();
      };

      this.ws.onmessage = (event) => {
        this.handleMessage(event.data as string);
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

  private handleMessage(raw: string): void {
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    switch (msg.type) {
      case "welcome": {
        this.epoch = msg.epoch as string;
        break;
      }

      case "event": {
        const sender = msg.sender as string;
        if (sender === this.config.app) return;

        const channel = msg.channel as string;
        const offset = msg.offset as number;

        this.offsets.set(channel, offset);

        const handlers = this.handlers.get(channel);
        if (handlers) {
          const meta: EventMeta = {
            id: msg.id as string,
            channel,
            offset,
            timestamp: msg.timestamp as number,
            sender,
          };
          for (const handler of handlers) {
            try {
              handler(msg.payload, meta);
            } catch (err) {
              this._onError?.(
                err instanceof Error ? err : new Error(String(err)),
              );
            }
          }
        }

        this.ws?.send(
          JSON.stringify({ type: "ack", channel, offset }),
        );
        break;
      }

      case "error": {
        this._onError?.(
          new Error(
            `Pool error [${msg.code}]: ${msg.message}`,
          ),
        );
        break;
      }

      case "pong":
        break;
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnecting) return;
    if (this.reconnectAttempt >= this.maxReconnectAttempts) {
      this._onError?.(new Error("Max reconnection attempts reached"));
      return;
    }

    this.reconnecting = true;
    const delay =
      Math.min(500 * Math.pow(2, this.reconnectAttempt), 30_000) +
      Math.random() * 500;
    this.reconnectAttempt++;

    this.reconnectTimer = setTimeout(async () => {
      try {
        await this.register();
        await this.openWebSocket();
      } catch {
        this.reconnecting = false;
        this.scheduleReconnect();
      }
    }, delay);
  }
}
