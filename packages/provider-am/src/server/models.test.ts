import { assert, describe, it } from "@effect/vitest";
import { ProviderInstanceId, type ModelSelection } from "@t3tools/contracts";

import { FAKE_MODEL_CATALOG as CATALOG } from "./fakeAm.testkit.ts";
import {
  AM_FALLBACK_MODELS,
  amModelChange,
  amSelectionTransition,
  amServerModels,
  parseAmModelSlug,
} from "./models.ts";

const INSTANCE_ID = ProviderInstanceId.make("am_fleet");
const select = (model: string, effort?: string): ModelSelection => ({
  instanceId: INSTANCE_ID,
  model,
  ...(effort ? { options: [{ id: "effort", value: effort }] } : {}),
});

const efforts = (slug: string) => {
  const descriptor = amServerModels(CATALOG)
    .find((model) => model.slug === slug)
    ?.capabilities?.optionDescriptors?.find((candidate) => candidate.id === "effort");
  return descriptor?.type === "select"
    ? {
        ids: descriptor.options.map((option) => option.id),
        defaultId: descriptor.options.find((option) => option.isDefault)?.id,
      }
    : undefined;
};

describe("amServerModels", () => {
  it("lists am default, then each provider's default model and its models", () => {
    const models = amServerModels(CATALOG);
    assert.deepStrictEqual(
      models.map((model) => [model.slug, model.name, model.subProvider]),
      [
        ["default", "am default", undefined],
        ["claude", "Claude Code", undefined],
        ["claude/opus", "Claude · Opus", "Claude"],
        ["claude/sonnet", "Claude · Sonnet", "Claude"],
        ["claude/claude-fable-5-1[1m]", "Claude · Fable 5.1 (1M context)", "Claude"],
        ["codex", "Codex", undefined],
        ["codex/gpt-6.1-sol", "Codex · GPT-6.1-Sol", "Codex"],
        ["codex/gpt-6-luna", "Codex · GPT-6-Luna", "Codex"],
      ],
    );
    assert.isTrue(models[0]!.isDefault);
  });

  it("gives Claude models the provider's efforts, led by the provider's own default", () => {
    const expected = {
      ids: ["default", "low", "medium", "high", "xhigh", "max"],
      defaultId: "default",
    };
    assert.deepStrictEqual(efforts("claude/opus"), expected);
    assert.deepStrictEqual(efforts("claude"), expected);
  });

  it("gives Codex models their own efforts with am's default", () => {
    assert.deepStrictEqual(efforts("codex/gpt-6.1-sol"), {
      ids: ["low", "medium", "high", "ultra"],
      defaultId: "medium",
    });
    assert.deepStrictEqual(efforts("codex/gpt-6-luna"), { ids: ["low", "high"], defaultId: "low" });
    // Codex lists no provider-wide efforts, so its default model offers none.
    assert.isUndefined(efforts("codex"));
    assert.isNull(amServerModels(CATALOG)[0]!.capabilities);
  });

  it("falls back to the provider defaults without a model list", () => {
    assert.strictEqual(amServerModels(undefined), AM_FALLBACK_MODELS);
    assert.deepStrictEqual(
      AM_FALLBACK_MODELS.map((model) => [model.slug, model.name, model.capabilities]),
      [
        ["default", "am default", null],
        ["claude", "Claude Code", null],
        ["codex", "Codex", null],
      ],
    );
  });
});

describe("am model selections", () => {
  it("parses a slug into what is sent to am", () => {
    assert.deepStrictEqual(parseAmModelSlug("claude/opus"), { provider: "claude", model: "opus" });
    assert.deepStrictEqual(parseAmModelSlug("codex"), { provider: "codex", model: null });
    assert.deepStrictEqual(parseAmModelSlug("default"), { provider: undefined, model: null });
  });

  it("restarts within a provider and hands off across providers", () => {
    assert.deepStrictEqual(amSelectionTransition(select("claude/opus"), select("claude/sonnet")), {
      type: "apply_on_next_turn",
    });
    assert.deepStrictEqual(amSelectionTransition(select("claude/opus"), select("claude")), {
      type: "apply_on_next_turn",
    });
    assert.deepStrictEqual(amSelectionTransition(select("claude/opus"), select("codex")), {
      type: "create_with_handoff",
    });
    // A thread on am default does not say which provider runs, so its turn checks.
    assert.deepStrictEqual(amSelectionTransition(select("default"), select("codex/gpt-6-luna")), {
      type: "apply_on_next_turn",
    });
    assert.strictEqual(
      amSelectionTransition(select("claude/opus"), select("default")).type,
      "reject",
    );
  });

  it("changes only what the selection asks for", () => {
    const opusHigh = { model: "opus", effort: "high" };
    assert.isUndefined(amModelChange(opusHigh, select("claude/opus")));
    assert.isUndefined(amModelChange(opusHigh, select("claude/opus", "high")));
    assert.deepStrictEqual(amModelChange(opusHigh, select("claude/opus", "max")), {
      effort: "max",
    });
    assert.deepStrictEqual(amModelChange(opusHigh, select("claude/opus", "default")), {
      effort: null,
    });
    // A new model without an effort runs its default, as the picker shows it.
    assert.deepStrictEqual(amModelChange(opusHigh, select("claude/sonnet")), {
      model: "sonnet",
      effort: null,
    });
    assert.deepStrictEqual(amModelChange({ model: null, effort: null }, select("claude/sonnet")), {
      model: "sonnet",
    });
    assert.deepStrictEqual(amModelChange(opusHigh, select("claude")), {
      model: null,
      effort: null,
    });
    assert.isUndefined(amModelChange(opusHigh, select("default")));
  });
});
