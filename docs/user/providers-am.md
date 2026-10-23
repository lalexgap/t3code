# Agent Motel

[Agent Motel](https://github.com/lalexgap/agent-motel) (`am`) runs Claude Code and Codex agents
in tmux sessions. The Agent Motel provider is a beta integration and is disabled by default. With
it, each T3 Code thread drives one am agent, so you can follow and steer your am fleet from the
web, desktop, and mobile apps.

## Set up

1. On the machine hosting your environment, run `am serve`. It prints its API token (`am token` prints it again) and listens on
   `http://127.0.0.1:8787` unless am's config sets another `apiPort`.
2. Open **Settings → Providers**, select the environment, and enable **Agent Motel**.
3. Paste the token into **API token**. Set **API URL** if `am serve` listens elsewhere. T3 Code
   sends the token to it, so point it only at your own am. It must use `https`; plain `http` is
   accepted only for this machine (`localhost`, `127.0.0.1`, or `::1`).
4. Refresh provider status. It only checks that am answers and accepts the token; it never starts
   an agent.

T3 Code needs an am with message receipts. With an older am, sending fails and asks you to update
it.

T3 Code connects to `am serve` from the environment's host, so devices you connect from do not
need to reach am themselves.

## Fleet hosts

To run agents on another machine in your am fleet, add another Agent Motel instance in
**Settings → Providers** and set **Fleet host** to that machine's name from am's `remotes`, for
example one instance for this machine and one named "am · laptop". Leave **Fleet host** blank for
the machine running `am serve`; its own name, or `local`, is refused. Status for a fleet host checks
whether am can reach it. Its turns update every few seconds rather than as am records them.

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
token by token. Messages that reach the agent from elsewhere, such as another agent or its
terminal, show as notes in the thread. Work the agent does between T3 Code turns shows as an
**Agent Motel activity** run while **Mirror agents** is on (see below).

If the agent is busy with other work, the message waits in am's queue and the turn starts when am
delivers it. Sending while the turn runs steers it. **Stop** withdraws a message am has not
delivered yet, and otherwise sends Esc to the agent only while that message's turn is running. If
the agent's session ends mid-turn, the turn fails.

## Settling and archiving

Settling a thread stops its agent, so a finished agent stops holding a session. Stopping ends the
agent's tmux session but keeps it in am, with its conversation and worktree. Unsettling starts
nothing; your next message resumes the agent and then delivers. Archiving leaves the agent running
unless you turn on **Stop agents when their thread is archived**. Both are in
**Settings → Providers → Agent Motel**, per instance; turn off **Stop agents when their thread is
settled** to keep agents running. The mobile app has no editor for these settings, so change them
from web or desktop.

An agent that is working, waiting on a prompt in its terminal, or holding scheduled work,
background shells or queued messages is never stopped: the thread shows a note saying so, and you
can stop it with `am stop <name>`. An agent another active thread still uses keeps running too.
am checks again as it stops the agent, so work that reaches it in the meantime keeps it running.
This needs a recent am, on the agent's host too; with an older one, the thread notes that the agent
was not stopped and asks you to update am.
The thread also notes when its agent was stopped, or had stopped already. A stopped agent keeps its
mirrored thread; T3 Code never removes an agent.

## Mirrored agents

T3 Code keeps a thread for each agent in am's fleet, including agents you start with `am new`.
**Settings → Providers → Agent Motel → Mirror agents** picks which: agents with a running session,
plus stopped agents active within the chosen window (7 days unless you change it). **Off** turns
mirroring off; threads that already exist stay. Each Agent Motel instance mirrors its own Fleet
host. Refresh provider status, or run **Sync Agent Motel agents** from the command palette, to sync
now; otherwise it syncs about every 30 seconds, or every minute for a fleet host. New agents import
one a second, running ones first. Provider status shows how many agents it mirrors and how many are
still importing.

- **Projects.** A new thread lands in the project for the agent's git repository, or its directory
  outside one, which T3 Code adds if you do not have it yet. An agent in a worktree, whether am
  made it or not, lands in the main repository's project, on that worktree. A git repository can
  be anywhere. A repository reached through a symlink is one project however am spells it: a
  local agent joins the project already at the same real path, and a new project takes the spelling
  under your home directory when one leads there, else the real path. An agent whose project would
  be `/`, your home directory, or a folder outside it that is not a git repository is skipped, and
  provider status counts it.
- **Fleet hosts.** A fleet host's paths are on that machine, so T3 Code uses this machine's copy at
  the same path under your home directory, as it does for threads it starts there. A fleet host's
  worktree stays on that host, so its agent's thread uses the project, not a worktree. It never creates
  a directory: an agent whose project has no copy here is not mirrored, and provider status counts
  it. Diffs and checkpoints read this machine's copy, so they do not show such an agent's changes.
- **History.** A new thread shows up to the agent's last 200 transcript entries as history, without
  tool calls, and fewer when their messages are long. Read older ones with `am transcript <name>`.
- **Activity.** Work that T3 Code did not send appears as it happens in an **Agent Motel activity**
  run, which lasts while the agent works. Sending a message during it waits for the agent instead
  of typing into its work. **Stop** on such a run stops showing it; it does not interrupt the agent.
- **Removed agents.** When am no longer has an agent, for example after `am rm`, its thread is
  archived. It is unarchived if the agent comes back, for example with `am restore`. A stopped
  agent that ages out of the window keeps its thread.
- **Your archive wins.** A thread you archive stays archived while its agent keeps running. It is
  unarchived only when the agent stops or disappears and later runs again.
- **Deleting a thread** keeps the agent in am, and T3 Code never mirrors that agent again. T3 Code
  never removes a worktree am created, even one you opened other threads in, and storage cleanup
  leaves it alone.

## Permissions and limitations

am starts agents with their provider's own permissions, which are unrestricted unless am's
`skipPermissions` is off. T3 Code offers only **Full access** for these threads and cannot answer an
agent's prompts. When an agent waits on one, the thread says so; answer it in the agent's session with `am j <name>`.

Leave `t3-…` agents to T3 Code: identical text typed or sent into one from outside T3 Code can be
taken for T3 Code's own message.

Plan mode, forks, conversation rewind, and T3 Code's tools are not available. Switching an
existing thread to Agent Motel passes its conversation as a handoff.

Deleting a thread never stops or removes its agent, and settling or archiving one only stops it
(see above). Remove an agent with `am rm <name>`. To stop using Agent Motel in an environment, disable it in
**Settings → Providers**.
