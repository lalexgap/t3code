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
        description: "Where `am serve` listens. Leave blank for the local default.",
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
  },
  { order: ["apiUrl", "apiToken"] },
);
export type AmSettings = typeof AmSettings.Type;

export const amApiUrl = (settings: Pick<AmSettings, "apiUrl">) =>
  (settings.apiUrl || AM_DEFAULT_API_URL).replace(/\/+$/, "");
