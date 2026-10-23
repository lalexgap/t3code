import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";

import { AmSettings, isAmApiUrlAllowed } from "../settings.ts";
import { makeAmApi } from "./api.ts";
import { FAKE_MODEL_CATALOG, FAKE_TOKEN, makeFakeAm } from "./fakeAm.testkit.ts";
import { checkAmProviderStatus, makeAmModelCatalogCache } from "./status.ts";

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

  it.live("reports a Fleet host that names the machine running am serve", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm({ hosts: [{ name: "gapserver", reachable: true }] });
      for (const host of ["local", "fake-local"]) {
        const status = yield* check(fake.url, FAKE_TOKEN, { host });
        assert.strictEqual(status.status, "error");
        assert.include(status.message ?? "", "Leave Fleet host blank");
      }
    }).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer)),
  );

  it.live("refuses to send the token over plain http to another machine", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAm();
      const status = yield* check(fake.url, FAKE_TOKEN, { apiUrl: "http://am.example:8787" });
      assert.strictEqual(status.status, "error");
      assert.include(status.message ?? "", "https://");
      assert.deepStrictEqual(fake.log, []);
      for (const apiUrl of [
        "http://127.0.0.1:8787",
        "http://127.8.0.1",
        "http://localhost:8787/",
        "http://[::1]:8787",
        "https://am.example",
      ])
        assert.isTrue(isAmApiUrlAllowed({ apiUrl }), apiUrl);
      for (const apiUrl of [
        "http://10.0.0.5:8787",
        "http://localhost.example",
        "http://127.0.0.1.example",
        "ftp://127.0.0.1",
      ])
        assert.isFalse(isAmApiUrlAllowed({ apiUrl }), apiUrl);
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

  describe("model list", () => {
    const withCatalog = Effect.fnUntraced(function* (
      fake: { readonly url: string },
      host?: string,
    ) {
      const settings = decodeSettings({ enabled: true, apiToken: FAKE_TOKEN, host: host ?? "" });
      const api = makeAmApi(yield* HttpClient.HttpClient, { baseUrl: fake.url, token: FAKE_TOKEN });
      const catalog = yield* makeAmModelCatalogCache(settings, api);
      return { catalog, check: checkAmProviderStatus(settings, api, catalog) };
    });
    const slugs = (status: { readonly models: ReadonlyArray<{ readonly slug: string }> }) =>
      status.models.map((model) => model.slug);

    it.live("reads am's list once, and again only on a model refresh", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeAm({ models: () => FAKE_MODEL_CATALOG });
        const { catalog, check } = yield* withCatalog(fake);
        const first = yield* check;
        assert.include(slugs(first), "claude/opus");
        assert.include(slugs(first), "codex/gpt-6.1-sol");
        yield* check;
        const reads = () => fake.log.filter((call) => call.path === "/api/models");
        assert.lengthOf(reads(), 1);
        yield* catalog.refresh;
        assert.deepStrictEqual(
          reads().map((call) => call.search),
          [undefined, "?refresh=1"],
        );
      }).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer)),
    );

    it.live("keeps the provider defaults for an am without a model list", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeAm();
        const { catalog, check } = yield* withCatalog(fake);
        assert.deepStrictEqual(slugs(yield* check), ["default", "claude", "codex"]);
        assert.isFalse(fake.log.some((call) => call.path === "/api/models"));
        // A refresh asks anyway, and its 404 keeps the defaults.
        yield* catalog.refresh;
        assert.deepStrictEqual(slugs(yield* check), ["default", "claude", "codex"]);
      }).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer)),
    );

    it.live("reads a fleet host's own list", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeAm({
          hosts: [{ name: "gapserver", reachable: true }],
          models: (host) =>
            host === "gapserver"
              ? [{ provider: "codex", models: [{ id: "gpt-6-luna", efforts: [] }], efforts: [] }]
              : FAKE_MODEL_CATALOG,
        });
        const { check } = yield* withCatalog(fake, "gapserver");
        assert.deepStrictEqual(slugs(yield* check), ["default", "codex", "codex/gpt-6-luna"]);
        assert.deepStrictEqual(
          fake.log.filter((call) => call.path === "/api/models").map((call) => call.search),
          ["?host=gapserver"],
        );
      }).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer)),
    );
  });
});
