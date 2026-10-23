import type {
  ModelCapabilities,
  ModelSelection,
  ProviderInstanceId,
  ProviderOptionChoice,
  ServerProviderModel,
} from "@t3tools/contracts";

import type { ProviderSelectionTransitionPlan } from "@t3tools/provider-core/server/selectionTransition";

import type { AmAgentDetail, AmModelCatalog, AmModelChange, AmProvider } from "./api.ts";

/** Let am pick its configured default provider. Never sent to am. */
export const AM_DEFAULT_MODEL = "default";

const AM_EFFORT_OPTION = "effort";
/** The effort choice that runs the provider's own default. Never sent to am. */
const AM_DEFAULT_EFFORT = "default";

const PROVIDERS: ReadonlyArray<AmProvider> = ["claude", "codex"];
const isAmProvider = (value: string): value is AmProvider =>
  PROVIDERS.some((provider) => provider === value);

const PROVIDER_LABELS: Record<AmProvider, string> = { claude: "Claude", codex: "Codex" };
const DEFAULT_MODEL_NAMES: Record<AmProvider, string> = { claude: "Claude Code", codex: "Codex" };

const EFFORT_LABELS: Record<string, string> = {
  none: "None",
  minimal: "Minimal",
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "Extra High",
  max: "Max",
  ultra: "Ultra",
};

const capitalize = (value: string) => value.charAt(0).toUpperCase() + value.slice(1);

/**
 * A model slug names the provider, and the model after a slash: `claude/opus`. A bare provider is
 * that provider's default model, and `default` is am's default provider.
 */
export const parseAmModelSlug = (
  slug: string,
): { readonly provider: AmProvider | undefined; readonly model: string | null } => {
  const slash = slug.indexOf("/");
  const provider = slash === -1 ? slug : slug.slice(0, slash);
  if (!isAmProvider(provider)) return { provider: undefined, model: null };
  return { provider, model: slash === -1 ? null : slug.slice(slash + 1) || null };
};

const amModelSlug = (provider: AmProvider, model: string | null | undefined) =>
  model ? `${provider}/${model}` : provider;

/**
 * The effort a selection asks for: a value, null for the provider's default, or undefined when it
 * names none.
 */
export const amSelectionEffort = (selection: ModelSelection): string | null | undefined => {
  const value = selection.options?.find((option) => option.id === AM_EFFORT_OPTION)?.value;
  if (typeof value !== "string") return undefined;
  return value === AM_DEFAULT_EFFORT ? null : value;
};

/** What the agent runs, as T3 reports a provider thread's native selection. */
export const amAgentSelection = (
  instanceId: ProviderInstanceId,
  detail: Pick<AmAgentDetail, "provider" | "model" | "effort">,
): ModelSelection | undefined =>
  isAmProvider(detail.provider)
    ? {
        instanceId,
        model: amModelSlug(detail.provider, detail.model),
        ...(detail.effort ? { options: [{ id: AM_EFFORT_OPTION, value: detail.effort }] } : {}),
      }
    : undefined;

const NO_EFFORTS: ModelCapabilities = { optionDescriptors: [] };

/** With no default from am, a leading Default choice leaves the effort to the provider. */
const effortCapabilities = (
  efforts: ReadonlyArray<string>,
  defaultEffort: string | undefined,
): ModelCapabilities => {
  if (efforts.length === 0) return NO_EFFORTS;
  const preset = defaultEffort !== undefined && efforts.includes(defaultEffort);
  const options: Array<ProviderOptionChoice> = [
    ...(preset ? [] : [{ id: AM_DEFAULT_EFFORT, label: "Default", isDefault: true }]),
    ...efforts.map((id) => ({
      id,
      label: EFFORT_LABELS[id] ?? capitalize(id),
      ...(preset && id === defaultEffort ? { isDefault: true } : {}),
    })),
  ];
  return {
    optionDescriptors: [
      {
        id: AM_EFFORT_OPTION,
        label: "Effort",
        type: "select",
        options,
        currentValue: preset ? defaultEffort : AM_DEFAULT_EFFORT,
      },
    ],
  };
};

/** am's label, else the id as Codex and Claude spell theirs: `GPT-6.1-Sol`, `Opus`. */
const modelName = (id: string, label: string | undefined) =>
  label?.trim() ||
  (/^gpt-/i.test(id)
    ? id
        .replace(/^gpt/i, "GPT")
        .replace(/-([a-z])/g, (_, letter: string) => `-${letter.toUpperCase()}`)
    : capitalize(id));

const AM_DEFAULT_ENTRY: ServerProviderModel = {
  slug: AM_DEFAULT_MODEL,
  name: "am default",
  isCustom: false,
  isDefault: true,
  capabilities: null,
};

/** What an am without a model list, or one T3 cannot reach, offers. */
export const AM_FALLBACK_MODELS: ReadonlyArray<ServerProviderModel> = [
  AM_DEFAULT_ENTRY,
  ...PROVIDERS.map((provider) => ({
    slug: provider,
    name: DEFAULT_MODEL_NAMES[provider],
    isCustom: false,
    capabilities: null,
  })),
];

/**
 * am default first, then each provider's default model and its listed models. A model with no
 * efforts of its own takes its provider's, as every Claude model does.
 */
export const amServerModels = (
  catalog: AmModelCatalog | undefined,
): ReadonlyArray<ServerProviderModel> => {
  if (catalog === undefined) return AM_FALLBACK_MODELS;
  const models: Array<ServerProviderModel> = [AM_DEFAULT_ENTRY];
  for (const provider of PROVIDERS) {
    const entry = catalog.find((candidate) => candidate.provider === provider);
    if (!entry) continue;
    models.push({
      slug: provider,
      name: DEFAULT_MODEL_NAMES[provider],
      isCustom: false,
      capabilities: effortCapabilities(entry.efforts, undefined),
    });
    const seen = new Set<string>();
    for (const model of entry.models) {
      const id = model.id.trim();
      if (!id || seen.has(id)) continue;
      seen.add(id);
      const name = modelName(id, model.label);
      models.push({
        slug: amModelSlug(provider, id),
        name: `${PROVIDER_LABELS[provider]} · ${name}`,
        shortName: name,
        subProvider: PROVIDER_LABELS[provider],
        isCustom: false,
        capabilities: effortCapabilities(
          model.efforts.length > 0 ? model.efforts : entry.efforts,
          model.defaultEffort,
        ),
      });
    }
  }
  return models;
};

const providerName = (provider: string) =>
  isAmProvider(provider) ? DEFAULT_MODEL_NAMES[provider] : provider;

/**
 * An agent keeps its provider: another model of it is a restart on the same conversation, while
 * another provider is a new agent that gets the thread as a handoff.
 */
export const amSelectionTransition = (
  current: ModelSelection,
  target: ModelSelection,
): ProviderSelectionTransitionPlan => {
  if (current.model === target.model) return { type: "apply_on_next_turn" };
  const to = parseAmModelSlug(target.model).provider;
  if (to === undefined)
    return {
      type: "reject",
      reason:
        "am default picks the provider only for a new agent. Pick Claude Code or Codex to run that provider's default model.",
    };
  // A thread started on am default does not say which provider its agent runs; the turn checks.
  const from = parseAmModelSlug(current.model).provider;
  return from === undefined || from === to
    ? { type: "apply_on_next_turn" }
    : { type: "create_with_handoff" };
};

/**
 * The restart that brings an existing agent to the selection, or undefined when it already runs
 * it. A new model without an effort of its own runs the provider's default effort, as the picker
 * shows it.
 */
export const amModelChange = (
  detail: Pick<AmAgentDetail, "model" | "effort">,
  selection: ModelSelection,
): AmModelChange | undefined => {
  const wanted = parseAmModelSlug(selection.model);
  if (wanted.provider === undefined) return undefined;
  const modelChanged = wanted.model !== (detail.model ?? null);
  const asked = amSelectionEffort(selection);
  const effort = asked !== undefined ? asked : modelChanged ? null : undefined;
  const effortChanged = effort !== undefined && effort !== (detail.effort ?? null);
  if (!modelChanged && !effortChanged) return undefined;
  return {
    ...(modelChanged ? { model: wanted.model } : {}),
    ...(effortChanged ? { effort } : {}),
  };
};

/** Why the selection cannot run on this agent at all, if it cannot. */
export const amProviderMismatch = (
  ref: string,
  detail: Pick<AmAgentDetail, "provider">,
  selection: ModelSelection,
) => {
  const wanted = parseAmModelSlug(selection.model).provider;
  if (wanted === undefined || detail.provider === wanted) return undefined;
  const runs = providerName(detail.provider);
  return `The Agent Motel agent ${ref} runs ${runs}, so it cannot switch to ${providerName(wanted)}. Pick a ${runs} model, or start a new thread.`;
};
