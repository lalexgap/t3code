import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";

import { AmSettings } from "./settings.ts";

export const amClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("am"),
  label: "Agent Motel",
  settingsSchema: AmSettings,
  badgeLabel: "Beta",
  icon: {
    viewBox: "0 0 24 24",
    fill: { light: "#0F0F0F", dark: "#F5F5F5" },
    paths: [
      {
        // A motel front: roof, three room doors, and an office door.
        d: "M2 10 12 3l10 7v11H2Zm3 2v3h3v-3Zm5.5 0v3h3v-3Zm5.5 0v3h3v-3Zm-6 5v4h4v-4Z",
        fillRule: "evenodd",
      },
    ],
  },
});
