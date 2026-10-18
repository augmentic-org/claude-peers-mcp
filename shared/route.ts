// Pure routing helpers for the Telegram bridge — no I/O, unit-tested.

export interface RoutablePeer {
  id: string; // broker peer id
  broker: string; // broker label ("local", "ws", ...)
  cwd: string;
  summary: string;
}

// Short human handle for a peer: the last path segment of its cwd, lowercased.
// Seats are opened at their seat path, so this is usually the seat name's tail
// (e.g. .../Products/Hio/frontend -> "frontend"). Off the local broker the
// broker label is prefixed ("ws:frontend") so the two machines never collide.
export function handleOf(p: RoutablePeer): string {
  const base = p.cwd.replace(/\/+$/, "").split("/").pop()?.toLowerCase() || p.id;
  return p.broker === "local" ? base : `${p.broker}:${base}`;
}

export type Resolution =
  | { kind: "one"; peer: RoutablePeer }
  | { kind: "none" }
  | { kind: "many"; peers: RoutablePeer[] };

// Resolve a target the human typed: exact peer id, exact handle, then handle
// prefix. Ambiguity is returned, never guessed.
export function resolve(target: string, peers: RoutablePeer[]): Resolution {
  const t = target.toLowerCase();
  const byId = peers.filter((p) => p.id === t);
  if (byId.length === 1) return { kind: "one", peer: byId[0]! };
  const exact = peers.filter((p) => handleOf(p) === t);
  if (exact.length === 1) return { kind: "one", peer: exact[0]! };
  if (exact.length > 1) return { kind: "many", peers: exact };
  const prefix = peers.filter((p) => handleOf(p).startsWith(t));
  if (prefix.length === 1) return { kind: "one", peer: prefix[0]! };
  if (prefix.length > 1) return { kind: "many", peers: prefix };
  return { kind: "none" };
}

export type Command =
  | { kind: "peers" }
  | { kind: "help" }
  | { kind: "whoami" }
  | { kind: "all"; text: string }
  | { kind: "to"; target: string; text: string }
  | { kind: "sticky"; text: string }; // no explicit target: last peer / reply-to

// Parse one inbound Telegram text.
//   /peers            list sessions
//   /all <msg>        broadcast
//   @<handle> <msg>   send to one session (also "/to <handle> <msg>")
//   <msg>             send to the replied-to / last-addressed session
export function parse(text: string): Command {
  const s = text.trim();
  const cmd = s.match(/^\/(\w+)(?:@\w+)?(?:\s+([\s\S]*))?$/);
  if (cmd) {
    const name = cmd[1]!;
    const rest = cmd[2] ?? "";
    switch (name.toLowerCase()) {
      case "peers":
      case "list":
        return { kind: "peers" };
      case "start":
      case "help":
        return { kind: "help" };
      case "whoami":
        return { kind: "whoami" };
      case "all":
        return { kind: "all", text: rest.trim() };
      case "to": {
        const m = rest.match(/^(\S+)\s+([\s\S]+)$/);
        if (m) return { kind: "to", target: m[1]!, text: m[2]!.trim() };
        return { kind: "help" };
      }
    }
  }
  const at = s.match(/^@(\S+)\s+([\s\S]+)$/);
  if (at) return { kind: "to", target: at[1]!, text: at[2]!.trim() };
  return { kind: "sticky", text: s };
}

// Telegram caps a message at 4096 chars; split on line boundaries where possible.
export function chunk(text: string, max = 4000): string[] {
  if (text.length <= max) return [text];
  const out: string[] = [];
  let rest = text;
  while (rest.length > max) {
    let cut = rest.lastIndexOf("\n", max);
    if (cut < max / 2) cut = max;
    out.push(rest.slice(0, cut));
    rest = rest.slice(cut).replace(/^\n/, "");
  }
  if (rest) out.push(rest);
  return out;
}
