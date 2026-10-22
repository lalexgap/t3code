# Agent Motel

[Agent Motel](https://github.com/lalexgap/agent-motel) (`am`) runs Claude Code and Codex agents
in tmux sessions. The Agent Motel provider is a beta integration and is disabled by default. With
it, each T3 Code thread drives one am agent, so you can follow and steer your am fleet from the
web, desktop, and mobile apps.

## Set up

1. On the machine hosting your environment, run `am serve`. It prints its API token (`am token` prints it again) and listens on
   `http://127.0.0.1:8787` unless am's config sets another `apiPort`.
2. Open **Settings → Providers**, select the environment, and enable **Agent Motel**.
3. Paste the token into **API token**. Set **API URL** if `am serve` listens elsewhere.
4. Refresh provider status. It only checks that am answers and accepts the token; it never starts
   an agent.

T3 Code connects to `am serve` from the environment's host, so devices you connect from do not
need to reach am themselves.

## Threads and agents

The first message in a thread starts an am agent named after the thread, such as
`t3-fix-login-4k2j9a`, in the thread's workspace or worktree. Later messages go to the same agent,
including after T3 Code restarts. If the agent has stopped, T3 Code resumes it.

Pick **Claude Code** or **Codex** in the model picker, or **am default** for the provider set in
am's config. An agent keeps its provider, so start a new thread to switch.

Replies, commands, and tool calls appear as am records them in the agent's transcript, not
token by token. Messages that reach the agent from elsewhere during a turn, such as another agent
or its terminal, show as notes in the thread. Activity between T3 Code turns is not shown; read it
with `am transcript <name>`.

Sending while the agent works steers its current turn. **Stop** sends Esc to the agent.

## Permissions and limitations

am starts agents with their provider's own permissions, which are unrestricted unless am's
`skipPermissions` is off. T3 Code offers only **Full access** for these threads and cannot answer an
agent's prompts. When an agent waits on one, the thread says so; answer it in the agent's session with `am j <name>`.

Plan mode, forks, conversation rewind, and T3 Code's tools are not available. Switching an
existing thread to Agent Motel passes its conversation as a handoff.

Deleting a thread leaves its agent in am. Remove it with `am rm <name>`. To stop using Agent
Motel in an environment, disable it in **Settings → Providers**.
