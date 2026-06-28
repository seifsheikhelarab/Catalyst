/// <reference lib="dom" />
const STORAGE_KEY = "catalyst_device_id";
const MAX_BATCH_SIZE = 20;
const FLUSH_INTERVAL_MS = 5000;
const MAX_RETRIES = 3;

interface AnalyticsOptions {
  apiKey: string;
  endpoint?: string;
}

interface EventPayload {
  event: string;
  timestamp: number;
  properties?: Record<string, unknown>;
  userId?: string;
  deviceId: string;
}

function uuid(): string {
  return crypto.randomUUID();
}

function getDeviceId(): string {
  if (typeof localStorage === "undefined") return uuid();
  let id = localStorage.getItem(STORAGE_KEY);
  if (!id) {
    id = uuid();
    localStorage.setItem(STORAGE_KEY, id);
  }
  return id;
}

export class Analytics {
  private apiKey: string;
  private endpoint: string;
  private deviceId: string;
  private userId: string = "";
  private traits: Record<string, unknown> = {};
  private batch: EventPayload[] = [];
  private flushTimer: ReturnType<typeof setInterval> | null = null;

  constructor(options: AnalyticsOptions) {
    this.apiKey = options.apiKey;
    this.endpoint = options.endpoint || "https://api.getcatalyst.dev/track";
    this.deviceId = getDeviceId();
    this.flushTimer = setInterval(() => this.flush(), FLUSH_INTERVAL_MS);
    if (typeof window !== "undefined") {
      window.addEventListener("beforeunload", () => this.flushSync());
    }
  }

  identify(userId: string, traits?: Record<string, unknown>): void {
    this.userId = userId;
    if (traits) this.traits = { ...this.traits, ...traits };
  }

  track(event: string, properties?: Record<string, unknown>): void {
    this.enqueue({ event, properties });
  }

  page(properties?: Record<string, unknown>): void {
    if (typeof window === "undefined") return;
    this.enqueue({
      event: "page_view",
      properties: {
        url: window.location.href,
        referrer: document.referrer || undefined,
        title: document.title,
        ...properties,
      },
    });
  }

  private enqueue(data: { event: string; properties?: Record<string, unknown> }): void {
    const payload: EventPayload = {
      event: data.event,
      timestamp: Date.now(),
      properties: data.properties,
      userId: this.userId || undefined,
      deviceId: this.deviceId,
    };
    this.batch.push(payload);
    if (this.batch.length >= MAX_BATCH_SIZE) {
      this.flush();
    }
  }

  async flush(): Promise<void> {
    if (this.batch.length === 0) return;
    const batch = this.batch.splice(0, MAX_BATCH_SIZE);
    let lastError: unknown;

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        const res = await fetch(this.endpoint, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${this.apiKey}`,
          },
          body: JSON.stringify(batch),
        });
        if (res.ok) return;
        lastError = new Error(`HTTP ${res.status}`);
      } catch (err) {
        lastError = err;
      }
      if (attempt < MAX_RETRIES - 1) {
        await new Promise((r) => setTimeout(r, Math.pow(2, attempt + 1) * 200));
      }
    }
    this.batch.unshift(...batch);
    console.warn("[catalyst] Failed to send events after", MAX_RETRIES, "retries", lastError);
  }

  private flushSync(): void {
    if (this.batch.length === 0) return;
    try {
      const payload = this.batch;
      this.batch = [];
      const data = navigator.sendBeacon
        ? navigator.sendBeacon(this.endpoint, JSON.stringify(payload))
        : false;
      if (!data) this.batch.unshift(...payload);
    } catch {
      // ignore sendBeacon errors during unload
    }
  }
}
