import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";

import { AmSettings } from "../settings.ts";
import { makeAmApi } from "./api.ts";
import { FAKE_TOKEN, makeFakeAm } from "./fakeAm.testkit.ts";
import { checkAmProviderStatus } from "./status.ts";

const decodeSettings = Schema.decodeSync(AmSettings);

const check = Effect.fnUntraced(function* (
  baseUrl: string,
  token: string,
  overrides: { readonly host?: string; readonly apiUrl?: string } = {},
) {
  const api = makeAmApi(yield* HttpClient.HttpClient, { baseUrl, token });
  return yield* checkAmProviderStatus(
    decodeSettings({ enabled: true, apiToken: token, ...overrides }),
    api,
  );
});

describe("checkAmProviderStatus", () => {
  it.live("reports a reachable am with a valid token as ready, and only checks health", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const status = yield* check(fake.url, FAKE_TOKEN);
      assert.strictEqual(status.status, "ready");
      assert.strictEqual(status.auth.status, "authenticated");
      yield* fake.takeCall("GET", "/api/health");
      assert.strictEqual(fake.agents.size, 0);
    }).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer)),
  );

  it.live("reports a rejected token as unauthenticated", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const status = yield* check(fake.url, "stale-token");
      assert.strictEqual(status.status, "error");
      assert.strictEqual(status.auth.status, "unauthenticated");
    }).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer)),
  );

  it.live("reports an unreachable am and a missing token honestly", () =>
    Effect.gen(function* () {
      const fake = yield* Effect.scoped(makeFakeAm());
      // The fake's port is closed once its scope ends.
      const unreachable = yield* check(fake.url, FAKE_TOKEN);
      assert.strictEqual(unreachable.status, "error");
      assert.strictEqual(unreachable.installed, false);
      assert.include(unreachable.message ?? "", "unreachable");

      const missing = yield* check(fake.url, "");
      assert.strictEqual(missing.status, "warning");
      assert.strictEqual(missing.auth.status, "unauthenticated");
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  );

  it.live("checks a fleet host through am's host list without touching agents", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm({
        hosts: [
          { name: "gapserver", reachable: true },
          { name: "laptop", reachable: false },
        ],
      });
      const ready = yield* check(fake.url, FAKE_TOKEN, { host: "gapserver" });
      assert.strictEqual(ready.status, "ready");
      const offline = yield* check(fake.url, FAKE_TOKEN, { host: "laptop" });
      assert.strictEqual(offline.status, "warning");
      assert.include(offline.message ?? "", "laptop");
      const unknown = yield* check(fake.url, FAKE_TOKEN, { host: "nowhere" });
      assert.strictEqual(unknown.status, "error");
      assert.include(unknown.message ?? "", "remotes");
      assert.isTrue(fake.log.every((call) => call.path === "/api/hosts"));
    }).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer)),
  );

  it.live("refuses to send the token to a non-http API URL", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const status = yield* check(fake.url, FAKE_TOKEN, { apiUrl: "ftp://am.example" });
      assert.strictEqual(status.status, "error");
      assert.include(status.message ?? "", "http");
      assert.deepStrictEqual(fake.log, []);
    }).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer)),
  );
});
