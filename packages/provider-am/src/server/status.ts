import type { ServerProviderModel } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";

import {
  buildServerProvider,
  DEFAULT_TIMEOUT_MS,
  type ProviderProbeResult,
} from "@t3tools/provider-core/server/snapshotProbe";
import {
  AM_API_URL_RULE,
  isAmApiUrlAllowed,
  isAmLocalHostName,
  type AmSettings,
} from "../settings.ts";
import type { AmApi, AmApiError, AmHealth, AmHosts, AmModelCatalog } from "./api.ts";
import { AM_FALLBACK_MODELS, amServerModels } from "./models.ts";

const AM_PRESENTATION = {
  displayName: "Agent Motel",
  badgeLabel: "Beta",
  showInteractionModeToggle: false,
  reportsContextWindow: false,
  // am launches each agent with its own provider's permissions (permissionless unless am's
  // skipPermissions is off). T3 cannot answer prompts in the agent's terminal, so it offers no
  // mode it would have to enforce itself.
  supportedRuntimeModes: ["full-access"],
} as const;

const snapshot = Effect.fnUntraced(function* (
  enabled: boolean,
  probe: ProviderProbeResult,
  models: ReadonlyArray<ServerProviderModel> = AM_FALLBACK_MODELS,
) {
  return buildServerProvider({
    presentation: AM_PRESENTATION,
    enabled,
    checkedAt: DateTime.formatIso(yield* DateTime.now),
    models,
    probe,
  });
});

export interface AmModelCatalogCache {
  /**
   * The cached list, read once on the first check that reaches am. `supported` is health's
   * `models` flag; a fleet host's is not known, so its first read finds out.
   */
  readonly current: (supported: boolean | undefined) => Effect.Effect<AmModelCatalog | undefined>;
  /** Re-reads the list, keeping the cached one when the read fails. */
  readonly refresh: Effect.Effect<void>;
}

/**
 * A status check runs every few minutes, and a fleet host's model list costs am an ssh round
 * trip, so the list is read once and then only on an explicit model refresh.
 */
export const makeAmModelCatalogCache = Effect.fnUntraced(function* (
  settings: Pick<AmSettings, "host">,
  api: Pick<AmApi, "models">,
) {
  const state = yield* Ref.make<{
    readonly catalog: AmModelCatalog | undefined;
    readonly read: boolean;
  }>({ catalog: undefined, read: false });
  const read = (refresh: boolean) =>
    api
      .models({
        ...(settings.host ? { host: settings.host } : {}),
        ...(refresh ? { refresh: true } : {}),
      })
      .pipe(
        Effect.timeout(DEFAULT_TIMEOUT_MS),
        Effect.tap((catalog) => Ref.set(state, { catalog, read: true })),
        Effect.catch((error) =>
          Effect.logWarning("Could not read Agent Motel's model list", error).pipe(
            Effect.andThen(Ref.update(state, (current) => ({ ...current, read: true }))),
          ),
        ),
        Effect.andThen(Ref.get(state)),
        Effect.map((current) => current.catalog),
      );
  return {
    current: (supported) =>
      supported === false
        ? Effect.succeed(undefined)
        : Ref.get(state).pipe(
            Effect.flatMap((current) =>
              current.read ? Effect.succeed(current.catalog) : read(false),
            ),
          ),
    refresh: read(true).pipe(Effect.asVoid),
  } satisfies AmModelCatalogCache;
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

/**
 * Only `GET /api/health`, or `GET /api/hosts` for a fleet host, and the model list the first time
 * am answers: a status check never starts or touches an agent.
 */
export const checkAmProviderStatus = Effect.fn("checkAmProviderStatus")(function* (
  settings: AmSettings,
  api: AmApi,
  catalog?: AmModelCatalogCache,
) {
  if (!settings.enabled) return yield* makePendingAmProvider(settings);
  if (!isAmApiUrlAllowed(settings))
    return yield* snapshot(true, {
      installed: false,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: AM_API_URL_RULE,
    });
  const localFleetHost = (host: string) =>
    snapshot(true, {
      installed: false,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: `Fleet host "${host}" is the machine running \`am serve\`. Leave Fleet host blank to run agents there.`,
    });
  if (settings.host && isAmLocalHostName(settings.host))
    return yield* localFleetHost(settings.host);
  if (!settings.apiToken)
    return yield* snapshot(true, {
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unauthenticated" },
      message: "Add the API token that `am serve` printed to connect to Agent Motel.",
    });
  // A fleet host's reachability comes from am's cached fleet state, so no ssh runs here.
  const probe: Effect.Effect<{ readonly hosts?: AmHosts; readonly health?: AmHealth }, AmApiError> =
    settings.host
      ? api.hosts.pipe(Effect.map((hosts) => ({ hosts })))
      : api.health.pipe(Effect.map((health) => ({ health })));
  const reached = yield* probe.pipe(Effect.timeout(DEFAULT_TIMEOUT_MS), Effect.result);
  if (Result.isSuccess(reached)) {
    const { hosts, health } = reached.success;
    if (settings.host && isAmLocalHostName(settings.host, hosts?.local))
      return yield* localFleetHost(settings.host);
    const fleetHost = hosts?.hosts.find((host) => host.name === settings.host);
    if (settings.host && !fleetHost)
      return yield* snapshot(true, {
        installed: false,
        version: null,
        status: "error",
        auth: { status: "authenticated" },
        message: `Agent Motel has no fleet host "${settings.host}". Add it to am's \`remotes\`.`,
      });
    if (fleetHost && !fleetHost.reachable)
      return yield* snapshot(true, {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "authenticated" },
        message: `Agent Motel cannot reach ${settings.host} right now.`,
      });
    return yield* snapshot(
      true,
      {
        installed: true,
        version: null,
        status: "ready",
        auth: { status: "authenticated" },
      },
      amServerModels(
        catalog === undefined
          ? undefined
          : yield* catalog.current(settings.host ? undefined : health?.models === true),
      ),
    );
  }
  const error = reached.failure;
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
      error._tag === "AmApiError" && error.status === 404 && settings.host
        ? "This Agent Motel has no fleet hosts API. Update am to use a fleet host."
        : error._tag === "AmApiError" && error.status !== null
          ? `Agent Motel answered HTTP ${error.status}. Check the API URL.`
          : "Agent Motel is unreachable. Start `am serve` on this host or check the API URL.",
  });
});
