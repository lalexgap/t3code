import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type * as ProviderThreadLifecycle from "@t3tools/provider-core/server/ProviderThreadLifecycle";
import { isAmApiUrlAllowed, type AmSettings } from "../settings.ts";
import type { AmApi } from "./api.ts";
import { amTurnPhase } from "./mapping.ts";

export interface AmThreadLifecycleOptions {
  readonly settings: AmSettings;
  readonly api: AmApi;
  /** Agents with a T3 turn running. */
  readonly busyAgents: ReadonlySet<string>;
}

/**
 * Stops a settled (or, when enabled, archived) thread's agent. Stopping only ends its tmux session;
 * the next message resumes it. A busy agent keeps running, even on work T3 did not send.
 */
export const makeAmThreadLifecycle = (
  options: AmThreadLifecycleOptions,
): ProviderThreadLifecycle.ProviderThreadLifecycleHandler => {
  const { settings, api } = options;
  const configured = settings.enabled && settings.apiToken !== "" && isAmApiUrlAllowed(settings);
  return Effect.fn("AmThreadLifecycle.change")(
    function* (change) {
      const wanted = change.event === "settled" ? settings.stopOnSettle : settings.stopOnArchive;
      if (!wanted || !configured) return null;
      const ref = change.nativeThreadId;
      const busy = (what: string) =>
        `The Agent Motel agent ${ref} ${what}, so it was not stopped when this thread was ${change.event}. Stop it with \`am stop ${ref}\`.`;
      if (change.sharedWithActiveThread)
        return `Another active thread uses the Agent Motel agent ${ref}, so it was not stopped.`;
      if (options.busyAgents.has(ref)) return busy("is working");
      const detail = yield* api.getAgent(ref);
      if (Option.isNone(detail)) return null;
      const phase = amTurnPhase(detail.value.status);
      // Noted so a repeat settle after a restart cannot stop an agent the user resumed since.
      if (phase === "gone") return `The Agent Motel agent ${ref} had already stopped.`;
      if (phase === "running") return busy("is working");
      if (phase === "attention") return busy("is waiting on a prompt in its terminal");
      if (detail.value.status === "waiting") return busy("has scheduled or background work");
      if ((detail.value.queue?.length ?? 0) > 0) return busy("has queued messages");
      // The user may have taken the thread back, and sent it work, while am answered.
      if (!(yield* change.stillApplies) || options.busyAgents.has(ref)) return null;
      const stopped = yield* api.stop(ref).pipe(
        Effect.as(true),
        // am no longer has the agent, so there is nothing left to stop.
        Effect.catchIf(
          (error) => error.status === 404,
          () => Effect.succeed(false),
        ),
      );
      return stopped ? `Stopped the Agent Motel agent ${ref}. Send a message to resume it.` : null;
    },
    (effect, change) =>
      effect.pipe(
        Effect.catch((error) =>
          Effect.logWarning(
            `Could not stop the Agent Motel agent ${change.nativeThreadId}`,
            error,
          ).pipe(
            Effect.as(
              `Could not stop the Agent Motel agent ${change.nativeThreadId}: ${error.message}`,
            ),
          ),
        ),
      ),
  );
};
