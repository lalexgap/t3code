/**
 * Shared by the server driver and the client settings form, so it holds only browser-safe schema
 * code.
 */
import { makeProviderSettingsSchema, secretSetting, TrimmedString } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/** `am serve` listens here unless its config sets another `apiPort`. */
const AM_DEFAULT_API_URL = "http://127.0.0.1:8787";

export const AmSettings = makeProviderSettingsSchema(
  {
    enabled: Schema.Boolean.pipe(
      Schema.withDecodingDefault(Effect.succeed(false)),
      Schema.annotateKey({ providerSettingsForm: { hidden: true } }),
    ),
    apiUrl: TrimmedString.pipe(
      Schema.withDecodingDefault(Effect.succeed("")),
      Schema.annotateKey({
        title: "API URL",
        description:
          "Where `am serve` listens. T3 Code sends the API token there, so it must use https unless it is on this machine. Leave blank for the local default.",
        providerSettingsForm: { placeholder: AM_DEFAULT_API_URL, clearWhenEmpty: "omit" },
      }),
    ),
    apiToken: TrimmedString.pipe(
      secretSetting,
      Schema.withDecodingDefault(Effect.succeed("")),
      Schema.annotateKey({
        title: "API token",
        description: "Printed by `am serve`. Stored separately on the host.",
        providerSettingsForm: { control: "password", clearWhenEmpty: "omit" },
      }),
    ),
    host: TrimmedString.pipe(
      Schema.withDecodingDefault(Effect.succeed("")),
      Schema.annotateKey({
        title: "Fleet host",
        description:
          "Run agents on this host from am's `remotes`. Leave blank for the machine running `am serve`.",
        providerSettingsForm: { placeholder: "this machine", clearWhenEmpty: "omit" },
      }),
    ),
  },
  { order: ["apiUrl", "apiToken", "host"] },
);
export type AmSettings = typeof AmSettings.Type;

export const amApiUrl = (settings: Pick<AmSettings, "apiUrl">) =>
  (settings.apiUrl || AM_DEFAULT_API_URL).replace(/\/+$/, "");

// `URL` normalizes the host, so `127.1` and `LOCALHOST` arrive here as `127.0.0.1` and `localhost`.
const isLoopbackHostname = (hostname: string) =>
  hostname === "localhost" || hostname === "[::1]" || /^127(\.\d{1,3}){3}$/.test(hostname);

/** The bearer token goes to this URL, so plain http is accepted only on the loopback interface. */
export const isAmApiUrlAllowed = (settings: Pick<AmSettings, "apiUrl">) => {
  if (!URL.canParse(amApiUrl(settings))) return false;
  const url = new URL(amApiUrl(settings));
  if (url.protocol === "https:") return url.hostname !== "";
  return url.protocol === "http:" && isLoopbackHostname(url.hostname);
};

export const AM_API_URL_RULE =
  "The Agent Motel API URL must use https://, or http:// only for localhost, 127.0.0.1 or ::1.";

/** am's `local` alias and its own host name both resolve to the machine running `am serve`. */
export const isAmLocalHostName = (host: string, local?: string) =>
  host === "local" || host === local;
