# Agent Motel

[Agent Motel](https://github.com/lalexgap/agent-motel) (`am`) runs Claude Code and Codex agents
in tmux sessions. The Agent Motel provider is a beta integration and is disabled by default. With
it, each T3 Code thread drives one am agent, so you can follow and steer your am fleet from the
web, desktop, and mobile apps.

## Set up

1. On the machine hosting your environment, run `am serve`. It prints its API token (`am token` prints it again) and listens on
   `http://127.0.0.1:8787` unless am's config sets another `apiPort`.
2. Open **Settings → Providers**, select the environment, and enable **Agent Motel**.
3. Paste the token into **API token**. Set **API URL** if `am serve` listens elsewhere. The URL
   must use `http` or `https`, and T3 Code sends the token to it, so point it only at your own am.
4. Refresh provider status. It only checks that am answers and accepts the token; it never starts
   an agent.

T3 Code needs an am with message receipts. With an older am, sending fails and asks you to update
it.

T3 Code connects to `am serve` from the environment's host, so devices you connect from do not
need to reach am themselves.

## Fleet hosts

To run agents on another machine in your am fleet, add another Agent Motel instance in
**Settings → Providers** and set **Fleet host** to that machine's name from am's `remotes`, for
example one instance for this machine and one named "am · laptop". Status for a fleet host checks
whether am can reach it.

The agent works in the fleet host's copy of the thread's project, at the same path under its home
directory. T3 Code's diffs and checkpoints read this machine's copy, so they do not show the
agent's changes. Review them on the fleet host, and keep both copies in sync yourself, for example
by pushing and pulling a branch.

## Threads and agents

The first message in a thread starts an am agent named after the thread, such as
`t3-fix-login-4k2j9a`, in the thread's workspace or worktree. Later messages go to the same agent,
including after T3 Code restarts. If the agent has stopped, T3 Code resumes it. T3 Code does not
take over an existing agent of that name that works in another directory.

Pick **Claude Code** or **Codex** in the model picker, or **am default** for the provider set in
am's config. An agent keeps its provider, so start a new thread to switch.

Replies, commands, and tool calls appear as am records them in the agent's transcript, not
token by token. Messages that reach the agent from elsewhere during a turn, such as another agent
or its terminal, show as notes in the thread. Activity between T3 Code turns is not shown; read it
with `am transcript <name>`.

If the agent is busy with other work, the message waits in am's queue and the turn starts when am
delivers it. Sending while the turn runs steers it. **Stop** withdraws a message am has not
delivered yet, and otherwise sends Esc to the agent.

## Permissions and limitations

am starts agents with their provider's own permissions, which are unrestricted unless am's
`skipPermissions` is off. T3 Code offers only **Full access** for these threads and cannot answer an
agent's prompts. When an agent waits on one, the thread says so; answer it in the agent's session with `am j <name>`.

Plan mode, forks, conversation rewind, and T3 Code's tools are not available. Switching an
existing thread to Agent Motel passes its conversation as a handoff.

Deleting a thread leaves its agent in am. Remove it with `am rm <name>`. To stop using Agent
Motel in an environment, disable it in **Settings → Providers**.
