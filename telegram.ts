#!/usr/bin/env bun
/**
 * claude-peers Telegram bridge
 *
 * ONE long-running daemon that owns ONE Telegram bot and joins the claude-peers
 * network as an ordinary peer. Every Claude Code session already running the
 * claude-peers channel can then be reached from Telegram, and can reply to (or
 * proactively ping) the human by send_message-ing the bridge's peer id.
 *
 * Why a single bridge and not the telegram channel plugin in each session:
 * Telegram allows exactly one getUpdates poller per bot token (a second gets
 * HTTP 409), so N sessions cannot share a bot. Session<->session traffic stays
 * on the broker; Telegram is only the human's window into it.
 *
 *   Telegram  ⇄  bridge (sole poller)  ⇄  broker(s)  ⇄  every session's channel
 *
 * Config (env, or ~/.config/claude-peers/telegram.env):
 *   TELEGRAM_BOT_TOKEN       BotFather token (required)
 *   TELEGRAM_ALLOWED_USERS   comma-separated numeric Telegram user ids. Empty =
 *                            locked: every sender just gets told its id.
 *   CLAUDE_PEERS_BROKERS     label=url,... (default local=http://127.0.0.1:7899)
 *                            e.g. local=http://127.0.0.1:7899,ws=http://127.0.0.1:7900
 *                            with `ssh -N -L 7900:127.0.0.1:7899 ws` for the WS broker.
 *
 * Run: bun telegram.ts
 */

import { existsSync, readFileSync } from "node:fs";
import type { Peer, PollMessagesResponse, RegisterResponse } from "./shared/types.ts";
import { chunk, handleOf, parse, resolve, type RoutablePeer } from "./shared/route.ts";

// --- Configuration ---

const ENV_FILE = `${process.env.HOME}/.config/claude-peers/telegram.env`;
if (existsSync(ENV_FILE)) {
  for (const line of readFileSync(ENV_FILE, "utf8").split("\n")) {
    const m = line.match(/^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*?)\s*$/);
    if (m && process.env[m[1]!] === undefined) process.env[m[1]!] = m[2]!.replace(/^["']|["']$/g, "");
  }
}

const TOKEN = process.env.TELEGRAM_BOT_TOKEN ?? "";
const ALLOWED = new Set(
  (process.env.TELEGRAM_ALLOWED_USERS ?? "").split(",").map((s) => s.trim()).filter(Boolean)
);
const BROKERS = (process.env.CLAUDE_PEERS_BROKERS ?? "local=http://127.0.0.1:7899")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean)
  .map((s) => {
    const [label = "", url = ""] = s.split("=");
    return { label: label.trim(), url: url.trim().replace(/\/+$/, ""), id: null as string | null };
  });

// TELEGRAM_API_BASE is for tests (a fake Bot API); never set it in real use.
const TG = `${process.env.TELEGRAM_API_BASE ?? "https://api.telegram.org"}/bot${TOKEN}`;
const OUTBOUND_POLL_MS = 1000;
const HEARTBEAT_MS = 15_000;
const SUMMARY =
  "Telegram bridge — the HUMAN on their phone. Messages from this peer are the user talking to you; " +
  "reply with send_message to this peer id and it reaches their Telegram.";

function log(msg: string) {
  console.error(`[claude-peers telegram] ${msg}`);
}

if (!TOKEN) {
  log(`TELEGRAM_BOT_TOKEN not set (env or ${ENV_FILE}). Exiting.`);
  process.exit(1);
}

// --- Broker ---

type Broker = (typeof BROKERS)[number];

async function brokerFetch<T>(b: Broker, path: string, body: unknown): Promise<T> {
  const res = await fetch(`${b.url}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(5000),
  });
  if (!res.ok) throw new Error(`broker ${b.label} ${path}: ${res.status} ${await res.text()}`);
  return res.json() as Promise<T>;
}

async function register(b: Broker) {
  // Local broker: real pid, so the broker's kill(pid,0) liveness check works.
  // Remote broker: our pid means nothing on that host — register a NEGATIVE pid,
  // which the broker treats as heartbeat-liveness instead of process-liveness.
  const pid = b.label === "local" ? process.pid : -process.pid;
  const reg = await brokerFetch<RegisterResponse>(b, "/register", {
    pid,
    cwd: "telegram",
    git_root: null,
    tty: null,
    summary: SUMMARY,
  });
  b.id = reg.id;
  log(`registered on ${b.label} as ${b.id}`);
}

async function listPeers(): Promise<RoutablePeer[]> {
  const out: RoutablePeer[] = [];
  for (const b of BROKERS) {
    if (!b.id) continue;
    try {
      const peers = await brokerFetch<Peer[]>(b, "/list-peers", {
        scope: "machine",
        cwd: "/",
        git_root: null,
        exclude_id: b.id,
      });
      for (const p of peers) {
        if (p.cwd === "telegram") continue; // another bridge
        out.push({ id: p.id, broker: b.label, cwd: p.cwd, summary: p.summary });
      }
    } catch (e) {
      log(`list-peers ${b.label}: ${(e as Error).message}`);
    }
  }
  return out;
}

async function sendToPeer(p: RoutablePeer, text: string, sender: string) {
  const b = BROKERS.find((x) => x.label === p.broker);
  if (!b?.id) throw new Error(`broker ${p.broker} not connected`);
  const r = await brokerFetch<{ ok: boolean; error?: string }>(b, "/send-message", {
    from_id: b.id,
    to_id: p.id,
    text: `[Telegram · ${sender}] ${text}`,
  });
  if (!r.ok) throw new Error(r.error ?? "send failed");
}

// --- Telegram ---

async function tg<T = any>(method: string, body: unknown, timeoutMs = 10_000): Promise<T> {
  const res = await fetch(`${TG}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const j = (await res.json()) as { ok: boolean; result: T; description?: string; error_code?: number };
  if (!j.ok) throw Object.assign(new Error(`${method}: ${j.description}`), { code: j.error_code });
  return j.result;
}

// Telegram message id -> the peer it came from, so replying to it routes back.
const replyMap = new Map<string, { broker: string; id: string }>();
// Last explicitly-addressed peer per chat, for un-prefixed follow-ups.
const sticky = new Map<number, { broker: string; id: string }>();

async function say(chatId: number, text: string, origin?: { broker: string; id: string }) {
  for (const part of chunk(text)) {
    const m = await tg<{ message_id: number }>("sendMessage", {
      chat_id: chatId,
      text: part,
      disable_web_page_preview: true,
    });
    if (origin) replyMap.set(`${chatId}:${m.message_id}`, origin);
  }
  if (replyMap.size > 2000) replyMap.delete(replyMap.keys().next().value!);
}

function peerLine(p: RoutablePeer): string {
  return `• ${handleOf(p)}  (${p.id})${p.summary ? `\n   ${p.summary.slice(0, 140)}` : ""}`;
}

const HELP =
  "Talk to your Claude sessions:\n" +
  "/peers — list live sessions\n" +
  "@<name> <msg> — send to one (name = its folder, prefix ok; ws:<name> for WS)\n" +
  "/all <msg> — broadcast to every session\n" +
  "<msg> — goes to the session you're replying to, else the last one you addressed\n" +
  "/whoami — your Telegram user id";

async function handleUpdate(u: any) {
  const msg = u.message ?? u.edited_message;
  if (!msg?.text || !msg.from) return;
  const chatId: number = msg.chat.id;
  const userId = String(msg.from.id);
  const sender = msg.from.username ? `@${msg.from.username}` : msg.from.first_name ?? userId;

  if (!ALLOWED.has(userId)) {
    // Locked by config: never route an unknown sender, just tell them their id so the
    // owner can add it to TELEGRAM_ALLOWED_USERS. Logged so you can spot strangers.
    log(`dropped message from non-allowlisted user ${userId} (${sender})`);
    await say(chatId, `Not authorised. Your Telegram user id is ${userId}.`);
    return;
  }

  const cmd = parse(msg.text);
  if (cmd.kind === "help") return say(chatId, HELP);
  if (cmd.kind === "whoami") return say(chatId, `Your Telegram user id is ${userId}.`);

  const peers = await listPeers();
  if (cmd.kind === "peers") {
    return say(chatId, peers.length ? peers.map(peerLine).join("\n") : "No live sessions.");
  }

  if (cmd.kind === "all") {
    if (!cmd.text) return say(chatId, "Usage: /all <message>");
    let n = 0;
    for (const p of peers) {
      try {
        await sendToPeer(p, cmd.text, sender);
        n++;
      } catch (e) {
        log(`broadcast to ${p.id}: ${(e as Error).message}`);
      }
    }
    return say(chatId, `→ sent to ${n}/${peers.length} sessions`);
  }

  let target: RoutablePeer | undefined;
  if (cmd.kind === "to") {
    const r = resolve(cmd.target, peers);
    if (r.kind === "none") return say(chatId, `No live session matches "${cmd.target}". /peers to list.`);
    if (r.kind === "many")
      return say(chatId, `"${cmd.target}" is ambiguous:\n${r.peers.map(peerLine).join("\n")}\nUse the id.`);
    target = r.peer;
  } else {
    const replied = msg.reply_to_message && replyMap.get(`${chatId}:${msg.reply_to_message.message_id}`);
    const ref = replied || sticky.get(chatId);
    target = ref && peers.find((p) => p.broker === ref.broker && p.id === ref.id);
    if (!target) return say(chatId, `Who to? Prefix with @<name>, or reply to a session's message.\n\n${HELP}`);
  }

  const text = cmd.text;
  try {
    await sendToPeer(target, text, sender);
    sticky.set(chatId, { broker: target.broker, id: target.id });
    await tg("setMessageReaction", {
      chat_id: chatId,
      message_id: msg.message_id,
      reaction: [{ type: "emoji", emoji: "👍" }],
    }).catch(() => say(chatId, `→ ${handleOf(target!)}`));
  } catch (e) {
    await say(chatId, `Send to ${handleOf(target)} failed: ${(e as Error).message}`);
  }
}

// --- Outbound: sessions -> Telegram ---

let pumping = false; // setInterval must not overlap a slow pump (would double-send)

async function pumpOutbound() {
  if (pumping) return;
  pumping = true;
  try {
    await pumpOnce();
  } finally {
    pumping = false;
  }
}

async function pumpOnce() {
  const chats = [...ALLOWED].map(Number); // private chat id == user id
  if (chats.length === 0) return;
  for (const b of BROKERS) {
    if (!b.id) continue;
    let res: PollMessagesResponse;
    try {
      res = await brokerFetch<PollMessagesResponse>(b, "/peek-messages", { id: b.id });
    } catch {
      continue;
    }
    if (res.messages.length === 0) continue;
    const peers = await listPeers();
    for (const m of res.messages) {
      const p = peers.find((x) => x.broker === b.label && x.id === m.from_id);
      const name = p ? handleOf(p) : `${b.label === "local" ? "" : b.label + ":"}${m.from_id}`;
      try {
        for (const c of chats) await say(c, `[${name}] ${m.text}`, { broker: b.label, id: m.from_id });
        // Ack only after Telegram accepted it, so a Telegram outage never drops mail.
        await brokerFetch(b, "/ack-messages", { id: b.id, message_ids: [m.id] });
      } catch (e) {
        log(`forward ${m.id} failed, will retry: ${(e as Error).message}`);
        return;
      }
    }
  }
}

// --- Liveness: re-register if a broker restarted and forgot us ---

async function heartbeat() {
  for (const b of BROKERS) {
    try {
      if (!b.id) {
        await register(b);
        continue;
      }
      await brokerFetch(b, "/heartbeat", { id: b.id });
      const all = await brokerFetch<Peer[]>(b, "/list-peers", { scope: "machine", cwd: "/", git_root: null });
      if (!all.some((p) => p.id === b.id)) {
        log(`${b.label} forgot us (broker restart?) — re-registering`);
        await register(b);
      }
    } catch (e) {
      b.id = null;
      log(`broker ${b.label} unreachable: ${(e as Error).message}`);
    }
  }
}

// --- Main ---

async function pollTelegram() {
  let offset = 0;
  for (;;) {
    try {
      const updates = await tg<any[]>(
        "getUpdates",
        { offset, timeout: 25, allowed_updates: ["message", "edited_message"] },
        35_000
      );
      for (const u of updates) {
        offset = u.update_id + 1;
        await handleUpdate(u).catch((e) => log(`update ${u.update_id}: ${(e as Error).message}`));
      }
    } catch (e) {
      const err = e as Error & { code?: number };
      if (err.code === 409) log("409: another process is polling this bot token — only one bridge may run per bot");
      else log(`getUpdates: ${err.message}`);
      await Bun.sleep(5000);
    }
  }
}

async function main() {
  const me = await tg<{ username: string }>("getMe", {});
  log(`bot @${me.username}; allowlist: ${ALLOWED.size ? [...ALLOWED].join(",") : "(empty — locked)"}`);
  await heartbeat();
  setInterval(heartbeat, HEARTBEAT_MS);
  setInterval(pumpOutbound, OUTBOUND_POLL_MS);

  const bye = async () => {
    for (const b of BROKERS) if (b.id) await brokerFetch(b, "/unregister", { id: b.id }).catch(() => {});
    process.exit(0);
  };
  process.on("SIGINT", bye);
  process.on("SIGTERM", bye);

  await pollTelegram();
}

main().catch((e) => {
  log(`fatal: ${(e as Error).message}`);
  process.exit(1);
});
