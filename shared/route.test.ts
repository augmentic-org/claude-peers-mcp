import { test, expect } from "bun:test";
import { handleOf, resolve, parse, chunk, type RoutablePeer } from "./route.ts";

const peers: RoutablePeer[] = [
  { id: "aaaa1111", broker: "local", cwd: "/x/Products/Hio/frontend", summary: "" },
  { id: "bbbb2222", broker: "local", cwd: "/x/Products/Hio/backend", summary: "" },
  { id: "cccc3333", broker: "ws", cwd: "/home/m/Products/Hio/frontend", summary: "" },
  { id: "dddd4444", broker: "local", cwd: "/x/Org/OpenSourced/", summary: "" },
];

test("handleOf uses cwd tail, prefixes non-local brokers", () => {
  expect(handleOf(peers[0]!)).toBe("frontend");
  expect(handleOf(peers[2]!)).toBe("ws:frontend");
  expect(handleOf(peers[3]!)).toBe("opensourced");
});

test("resolve: id, exact handle, prefix, ambiguity, none", () => {
  expect(resolve("bbbb2222", peers)).toEqual({ kind: "one", peer: peers[1]! });
  expect(resolve("frontend", peers)).toEqual({ kind: "one", peer: peers[0]! });
  expect(resolve("ws:front", peers)).toEqual({ kind: "one", peer: peers[2]! });
  expect(resolve("open", peers)).toEqual({ kind: "one", peer: peers[3]! });
  expect(resolve("zzz", peers)).toEqual({ kind: "none" });
  const dup = [...peers, { ...peers[0]!, id: "eeee5555" }];
  expect(resolve("frontend", dup).kind).toBe("many");
});

test("parse commands", () => {
  expect(parse("/peers")).toEqual({ kind: "peers" });
  expect(parse("/peers@MyBot")).toEqual({ kind: "peers" });
  expect(parse("/all ship it")).toEqual({ kind: "all", text: "ship it" });
  expect(parse("@frontend run tests\nplease")).toEqual({ kind: "to", target: "frontend", text: "run tests\nplease" });
  expect(parse("/to ws:backend status?")).toEqual({ kind: "to", target: "ws:backend", text: "status?" });
  expect(parse("what's next")).toEqual({ kind: "sticky", text: "what's next" });
});

test("chunk splits long text under the limit", () => {
  const long = Array.from({ length: 300 }, (_, i) => `line ${i} ${"x".repeat(40)}`).join("\n");
  const parts = chunk(long, 4000);
  expect(parts.length).toBeGreaterThan(1);
  for (const p of parts) expect(p.length).toBeLessThanOrEqual(4000);
  expect(parts.join("\n")).toBe(long);
});
