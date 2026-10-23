import { ProviderDriverKind, TextGenerationError } from "@t3tools/contracts";
import * as NodeOS from "node:os";

import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as ProviderThreadMirror from "@t3tools/provider-core/server/ProviderThreadMirror";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import { spawnAndCollect } from "@t3tools/provider-core/server/snapshotProbe";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import { withInstanceIdentity } from "@t3tools/provider-core/server/instanceIdentity";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import { makeManualOnlyProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenanceResolver";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "@t3tools/provider-core/server/snapshotSettings";
import type { ProviderTextGeneration } from "@t3tools/provider-core/server/textGeneration";
import { AmSettings, amApiUrl } from "../settings.ts";
import { makeAmAdapterV2 } from "./adapter.ts";
import { makeAmApi } from "./api.ts";
import { AmGitRootError, makeAmFleetMirror } from "./fleetMirror.ts";
import { amGitToplevel } from "./mirrorPlan.ts";
import { checkAmProviderStatus, makePendingAmProvider } from "./status.ts";

const DRIVER_KIND = ProviderDriverKind.make("am");
const decodeAmSettings = Schema.decodeSync(AmSettings);
const MAINTENANCE = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});

// Commit messages, titles, and the like would each need a throwaway am agent.
const unsupportedTextGeneration: ProviderTextGeneration = (() => {
  const unsupported = (operation: string) =>
    Effect.fail(
      new TextGenerationError({
        operation,
        detail: "Agent Motel instances do not provide application text generation.",
      }),
    );
  return {
    generateCommitMessage: () => unsupported("generateCommitMessage"),
    generatePrContent: () => unsupported("generatePrContent"),
    generateBranchName: () => unsupported("generateBranchName"),
    generateThreadTitle: () => unsupported("generateThreadTitle"),
    generateReviewGuide: () => unsupported("generateReviewGuide"),
  };
})();

export type AmDriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | ProviderHost.ProviderHost
  | HttpClient.HttpClient;

/** Null only when git cleanly reports no repository; a timeout or failed spawn fails. */
export const amGitRoot = (path: string) =>
  spawnAndCollect(
    "git",
    ChildProcess.make("git", [
      "-c",
      "core.fsmonitor=false",
      "-C",
      path,
      "rev-parse",
      "--show-prefix",
    ]),
  ).pipe(
    Effect.map((result) => (result.code === 0 ? amGitToplevel(path, result.stdout) : null)),
    Effect.timeout("5 seconds"),
    Effect.mapError((cause) => new AmGitRootError({ path, cause })),
  );

export const AmDriver: ProviderDriver<AmSettings, AmDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: { displayName: "Agent Motel", supportsMultipleInstances: true },
  configSchema: AmSettings,
  defaultConfig: () => decodeAmSettings({}),
  create: Effect.fn("AmDriver.create")(function* ({
    instanceId,
    displayName,
    accentColor,
    enabled,
    config,
  }) {
    const httpClient = yield* HttpClient.HttpClient;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const fileSystem = yield* FileSystem.FileSystem;
    const realPath = (path: string) =>
      fileSystem.realPath(path).pipe(Effect.orElseSucceed(() => null));
    const gitRoot = (path: string) =>
      amGitRoot(path).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
    const home = NodeOS.homedir();
    const effectiveConfig = { ...config, enabled } satisfies AmSettings;
    const api = makeAmApi(httpClient, {
      baseUrl: amApiUrl(effectiveConfig),
      token: effectiveConfig.apiToken,
    });
    const continuationIdentity = defaultProviderContinuationIdentity({
      driverKind: DRIVER_KIND,
      instanceId,
    });
    const stampIdentity = withInstanceIdentity({
      instanceId,
      driverKind: DRIVER_KIND,
      displayName,
      accentColor,
      continuationGroupKey: continuationIdentity.continuationKey,
    });
    const busyAgents = new Set<string>();
    const fleetMirror = yield* makeAmFleetMirror({
      instanceId,
      settings: effectiveConfig,
      api,
      mirror: yield* ProviderThreadMirror.ProviderThreadMirror,
      continuationRequests: yield* ProviderContinuationRequests.ProviderContinuationRequests,
      busyAgents,
      home,
      realHome: (yield* realPath(home)) ?? home,
      gitRoot,
      realPath,
    });
    const snapshotSettings = yield* makeProviderSnapshotSettingsSource(effectiveConfig);
    const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<AmSettings>>({
      resolveMaintenance: () => Effect.succeed(MAINTENANCE),
      getSettings: snapshotSettings.getSettings,
      streamSettings: snapshotSettings.streamSettings,
      haveSettingsChanged: haveProviderSnapshotSettingsChanged,
      initialSnapshot: (settings) =>
        makePendingAmProvider(settings.provider).pipe(Effect.map(stampIdentity)),
      // A status refresh, including Refresh in settings, also reconciles the fleet.
      checkProvider: checkAmProviderStatus(effectiveConfig, api).pipe(
        Effect.map(stampIdentity),
        Effect.tap(() => fleetMirror.wake),
      ),
      enrichSnapshot: ({ snapshot, publishSnapshot }) =>
        snapshot.status === "ready" && !snapshot.message
          ? fleetMirror.status.pipe(
              Stream.runForEach((message) =>
                publishSnapshot(message ? { ...snapshot, message } : snapshot),
              ),
            )
          : Effect.void,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({
            driver: DRIVER_KIND,
            instanceId,
            detail: "Failed to build the Agent Motel snapshot.",
            cause,
          }),
      ),
    );
    const orchestrationAdapter = yield* makeAmAdapterV2({
      instanceId,
      settings: effectiveConfig,
      api,
      busyAgents,
    });
    return {
      instanceId,
      driverKind: DRIVER_KIND,
      continuationIdentity,
      displayName,
      accentColor,
      enabled,
      snapshot,
      orchestrationAdapter,
      textGeneration: unsupportedTextGeneration,
    } satisfies ProviderInstance;
  }),
};
