import type { ProviderInstanceId, ProviderThreadId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";

/**
 * Tells a provider whose agents outlive T3 Code's sessions that the user is done with a thread,
 * after the settle or archive committed.
 */

export interface ProviderThreadLifecycleChange {
  readonly event: "settled" | "archived";
  readonly threadId: ThreadId;
  readonly providerThreadId: ProviderThreadId;
  readonly nativeThreadId: string;
  /** Another thread that is neither settled, archived nor deleted runs on the same native thread. */
  readonly sharedWithActiveThread: boolean;
  /**
   * Whether the thread is still settled or archived. The user can take it back while the provider
   * is still deciding, so check this right before acting.
   */
  readonly stillApplies: Effect.Effect<boolean>;
}

/** Resolves to a notice for the thread, or null when there is nothing to tell the user. */
export type ProviderThreadLifecycleHandler = (
  change: ProviderThreadLifecycleChange,
) => Effect.Effect<string | null>;

/**
 * The default reference drops every change, keeping driver construction dependency-free in tests.
 * The live layer must be shared with the server service that reports changes, like
 * `ProviderThreadMirror`.
 */
export class ProviderThreadLifecycle extends Context.Reference<{
  /** Handles one instance's threads until the scope closes. */
  readonly register: (
    instanceId: ProviderInstanceId,
    handler: ProviderThreadLifecycleHandler,
  ) => Effect.Effect<void, never, Scope.Scope>;
  readonly handles: (instanceId: ProviderInstanceId) => Effect.Effect<boolean>;
  readonly notify: (
    instanceId: ProviderInstanceId,
    change: ProviderThreadLifecycleChange,
  ) => Effect.Effect<string | null>;
}>("@t3tools/provider-core/server/ProviderThreadLifecycle", {
  defaultValue: () => ({
    register: () => Effect.void,
    handles: () => Effect.succeed(false),
    notify: () => Effect.succeed(null),
  }),
}) {}

export const layer = Layer.sync(ProviderThreadLifecycle, () => {
  const handlers = new Map<ProviderInstanceId, ProviderThreadLifecycleHandler>();
  return {
    register: (instanceId, handler) =>
      Effect.acquireRelease(
        Effect.sync(() => handlers.set(instanceId, handler)),
        // An instance rebuilt with new settings may register before the old one's scope closes.
        () =>
          Effect.sync(() => {
            if (handlers.get(instanceId) === handler) handlers.delete(instanceId);
          }),
      ).pipe(Effect.asVoid),
    handles: (instanceId) => Effect.sync(() => handlers.has(instanceId)),
    notify: (instanceId, change) =>
      Effect.suspend(() => handlers.get(instanceId)?.(change) ?? Effect.succeed(null)),
  };
});
