import type { ServerProviderModel } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";

import {
  buildServerProvider,
  DEFAULT_TIMEOUT_MS,
  type ProviderProbeResult,
} from "@t3tools/provider-core/server/snapshotProbe";
import type { AmSettings } from "../settings.ts";
import type { AmApi } from "./api.ts";
import { AM_DEFAULT_MODEL } from "./mapping.ts";

const AM_PRESENTATION = {
  displayName: "Agent Motel",
  badgeLabel: "Beta",
  showInteractionModeToggle: false,
  reportsContextWindow: false,
  // am launches each agent with its own provider's permissions (permissionless unless am's
  // skipPermissions is off). T3 cannot answer prompts in the agent's terminal, so it offers no
  // mode it would have to enforce itself.
  supportedRuntimeModes: ["full-access"],
  // An agent's provider is fixed when am starts it.
  requiresNewThreadForModelChange: true,
} as const;

const AM_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: AM_DEFAULT_MODEL,
    name: "am default",
    isCustom: false,
    isDefault: true,
    capabilities: null,
  },
  { slug: "claude", name: "Claude Code", isCustom: false, capabilities: null },
  { slug: "codex", name: "Codex", isCustom: false, capabilities: null },
];

const snapshot = Effect.fnUntraced(function* (enabled: boolean, probe: ProviderProbeResult) {
  return buildServerProvider({
    presentation: AM_PRESENTATION,
    enabled,
    checkedAt: DateTime.formatIso(yield* DateTime.now),
    models: AM_MODELS,
    probe,
  });
});

export const makePendingAmProvider = (settings: AmSettings) =>
  snapshot(settings.enabled, {
    installed: false,
    version: null,
    status: "warning",
    auth: { status: "unknown" },
    message: settings.enabled
      ? "Checking the Agent Motel API..."
      : "Agent Motel is disabled in T3 Code settings.",
  });

/** Only `GET /api/health`: a status check never starts or touches an agent. */
export const checkAmProviderStatus = Effect.fn("checkAmProviderStatus")(function* (
  settings: AmSettings,
  api: AmApi,
) {
  if (!settings.enabled) return yield* makePendingAmProvider(settings);
  if (!settings.apiToken)
    return yield* snapshot(true, {
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unauthenticated" },
      message: "Add the API token that `am serve` printed to connect to Agent Motel.",
    });
  const health = yield* api.health.pipe(Effect.timeout(DEFAULT_TIMEOUT_MS), Effect.result);
  if (Result.isSuccess(health))
    return yield* snapshot(true, {
      installed: true,
      version: null,
      status: "ready",
      auth: { status: "authenticated" },
    });
  const error = health.failure;
  if (error._tag === "AmApiError" && error.status === 401)
    return yield* snapshot(true, {
      installed: true,
      version: null,
      status: "error",
      auth: { status: "unauthenticated" },
      message: "Agent Motel rejected the API token. Copy the current one from `am serve`.",
    });
  return yield* snapshot(true, {
    installed: false,
    version: null,
    status: "error",
    auth: { status: "unknown" },
    message:
      error._tag === "AmApiError" && error.status !== null
        ? `Agent Motel answered HTTP ${error.status}. Check the API URL.`
        : "Agent Motel is unreachable. Start `am serve` on this host or check the API URL.",
  });
});
