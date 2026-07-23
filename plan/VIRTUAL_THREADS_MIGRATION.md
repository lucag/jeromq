# JeroMQ Architecture & Virtual-Thread Migration Notes

Notes from a conversation exploring JeroMQ's core architecture, whether it could
adopt Java's asynchronous channel APIs, and — once the real goals were clarified
(virtual threads + better ergonomics for other JVM languages like Scala) — a
concrete scoping of a virtual-thread migration and an assessment of existing test
coverage to gauge correctness of such a rewrite.

## 1. What are the core classes of this implementation?

JeroMQ mirrors libzmq's architecture closely. The public API (`org.zeromq.ZMQ`,
`ZContext`, `ZMsg`, `ZFrame`, etc.) is a thin wrapper; the actual implementation
lives in the `zmq` package.

### Core foundation classes (`zmq` package)

**Object/actor model**
- **`Ctx`** — the global context. Owns all sockets, I/O threads, and the reaper;
  the root of the whole system (~920 lines).
- **`ZObject`** — base class giving every internal object (sockets, sessions,
  pipes, engines) its actor-style command mailbox and the ability to send/receive
  `Command`s across threads.
- **`Own`** — base class for objects with ownership/lifecycle semantics
  (parent/child termination protocol — ZMQ's clean-shutdown logic).
- **`Command`** — the message struct passed between actors (bind, activate,
  plug, term, etc.), the backbone of ZMQ's internal concurrency model.

**Sockets**
- **`SocketBase`** — the abstract base every socket type (`Req`, `Rep`, `Pub`,
  `Sub`, `Router`, `Dealer`, `Push`, `Pull`, `Pair`, `Radio`, `Dish`, `Client`,
  `Server`, `Stream`...) extends. Implements common send/recv, option handling,
  and endpoint management (~1673 lines — arguably *the* central class).
- **`zmq/socket/*`** subpackages (`reqrep`, `pubsub`, `pipeline`, `clientserver`,
  `radiodish`, `scattergather`) — concrete socket-pattern implementations, plus
  helpers like `FQ` (fair queueing) and `LB` (load balancing).

**Messaging plumbing**
- **`Msg`** — the wire-level message/frame representation.
- **`pipe/Pipe`** and **`pipe/YPipe`/`YQueue`** — the lock-free-ish in-process
  queues connecting a socket to its sessions/engines.

**I/O and threading**
- **`IOThread`** — a dedicated thread running a `Poller` that drives
  sessions/engines.
- **`poll/Poller`** and **`poll/PollerBase`** — the selector-based event loop
  everything I/O-related runs on.
- **`io/SessionBase`** — sits between a socket's pipe and the network engine;
  manages connection lifecycle per peer.
- **`io/StreamEngine`** — the biggest single piece of protocol logic
  (~1311 lines): implements the ZMTP wire handshake, greeting, and framing over
  a raw socket.
- **`io/coder/*`** (`Encoder`/`Decoder`, v1/v2 variants) — ZMTP frame
  encoding/decoding.
- **`io/mechanism/*`** (`NullMechanism`, `plain`, `curve`, `gssapi`) — ZMTP
  security handshake mechanisms.
- **`io/net/*`** (`tcp`, `ipc`, `inproc`, `tipc`, `pgm`, `norm`) —
  transport-specific listener/connecter implementations.

**Supporting infrastructure**
- **`Mailbox`/`MailboxSafe`**, **`Signaler`** — cross-thread wakeup/signaling
  used by the actor mailboxes.
- **`Reaper`** — the actor responsible for terminating and cleaning up
  sockets/threads on context shutdown.
- **`Options`** — the (huge) bag of per-socket configuration used everywhere.

**One-sentence version:** `Ctx` + `SocketBase` + `ZObject`/`Own`/`Command`
(actor/lifecycle model) + `Pipe` (message transport) + `StreamEngine`/coders/
mechanisms (wire protocol) + `IOThread`/`Poller` (event loop) together *are* the
ZMTP implementation; everything in `org.zeromq` just calls down into them.

## 2. Is `SocketBase` truly asynchronous (does it use Java's Asynchronous Channel APIs)?

**No.** Confirmed by search — zero matches anywhere in the main source tree for
`AsynchronousSocketChannel`, `AsynchronousServerSocketChannel`,
`AsynchronousChannelGroup`, or `CompletionHandler`.

Instead JeroMQ uses classic non-blocking NIO:
- `SocketBase` works with plain `java.nio.channels.SocketChannel` /
  `SelectableChannel` (`SocketBase.java:5-6`).
- `zmq/poll/Poller` runs a `Selector`-based reactor loop (`select()`,
  `SelectionKey` sets) — the same readiness-based model as libzmq's own
  `epoll`/`kqueue`/`select` backends.
- Each `IOThread` owns a `Poller` and dispatches readiness events to
  `StreamEngine`/`SessionBase` instances, which do non-blocking `read()`/
  `write()` when the selector says a channel is ready.

So it's "asynchronous" only in the older, readiness-based reactor sense (like
libzmq itself, and like classic Netty/NIO servers) — not in the AIO/proactor
sense of `AsynchronousSocketChannel`, where the OS/JVM notifies you via a
completion handler when an I/O operation actually finishes. This is a
deliberate architectural mirror of libzmq's C reactor design (its
`epoll_t`/`poller_base` model ported onto `java.nio.channels.Selector`), not an
oversight.

## 3. Effort to rewrite the core using Java's Asynchronous Channel APIs

**Assessment: large — probably a full rewrite of the reactor core, and it
doesn't cleanly stay "just the core."** The main blocker isn't API mechanics,
it's that ZMQ's flow-control model depends on readiness semantics that
`Asynchronous*Channel` doesn't have.

Specific frictions:

1. **Pollin/pollout as backpressure, not just readiness.** `StreamEngine`
   constantly arms/disarms read and write interest (`setPollin`/`resetPollin`,
   same for pollout) to implement HWM and flow control — "don't even try to
   read until the peer/pipe has room." Async channels don't have an
   "arm but don't fire" concept: calling `read()` immediately kicks off a real
   read into a buffer. Restructuring flow control around cancelling in-flight
   operations changes the semantics, not just the plumbing.

2. **Thread-affinity/lock-free invariant.** Each `IOThread`'s `Poller` owns one
   thread, and every `SessionBase`/`StreamEngine` it drives is only ever
   touched from that thread — hence no locking on their internal state.
   `AsynchronousChannelGroup` completion handlers run on a JDK-managed pool (or
   the initiating thread, OS-dependent); pinning it to one thread per group is
   possible but then you've rebuilt a single-threaded reactor on top of
   infrastructure designed for pooled dispatch, buying nothing.

3. **Transport coverage gap.** Only TCP (and maybe IPC via Unix domain
   sockets) has an async-channel equivalent. `inproc` has no channel at all,
   and PGM/NORM are datagram-based. Since `StreamEngine`/`SessionBase`/
   `Poller`/`IOThread` are transport-agnostic and shared across all of these,
   you can't swap the reactor under just one transport without forking the
   core or leaving it half-migrated.

4. **Unclear payoff.** On Linux, `AsynchronousChannelGroup` is itself backed by
   epoll under the hood — so you'd be trading a well-understood single-thread
   epoll reactor for a JDK-managed thread pool with extra buffer/completion
   hops, likely *hurting* the low-latency characteristics ZMQ is chosen for.

**Recommendation reached: don't.** The Selector-reactor model isn't a
limitation being worked around — it's the correct tool for readiness-driven
flow control, and it's what libzmq itself uses (via epoll/kqueue).

## 4. Clarifying the actual goals

The real goals turned out to be:

1. **Modernizing for virtual/green threads** (Project Loom).
2. **Making the library more ergonomic**, especially for other JVM languages
   like Scala.

This changes the calculus a lot — virtual threads make the reactor pattern
(and the async-channel rewrite above) largely unnecessary, and the two goals
are complementary rather than each requiring core surgery.

### Virtual threads — a better fit than async channels, and cheaper

The Loom idiom is "blocking is the new async": with virtual threads, you don't
need a `Selector` multiplexing many connections on one carrier thread — you
give each connection its own virtual thread doing plain blocking
`SocketChannel.read()`/`write()`, and the JVM parks/unmounts it for free while
blocked. That eliminates the entire reason `Poller`/`IOThread` exist as a
shared-reactor abstraction.

Concretely this touches:
- **`Poller`/`IOThread`** — mostly deleted. Each `SessionBase`/`StreamEngine`
  gets `Thread.ofVirtual().start(...)` instead of being scheduled by a shared
  selector loop.
- **`StreamEngine`** — the `setPollin`/`resetPollin`/`setPollout`/
  `resetPollout` arm-disarm dance goes away entirely; flow control becomes a
  blocking put/take on the `Pipe`'s bounded queue, which naturally suspends the
  virtual thread when full. This is a *simplification*, not just a port.
- **Pinning audit** — only 6 `synchronized` usages in the whole `zmq` core
  (`SocketBase`, `Ctx`), so pinning risk is small and mostly moot if targeting
  JDK 24+ (JEP 491 removed `synchronized`-pinning).
- One reassuring fact: the *public* API (`Mailbox.recv`, `SocketBase.recv`/
  `send`) already blocks the calling thread today
  (`Mailbox.java:44-64`, via `signaler.waitEvent`;
  `SocketBase.processCommands`, `SocketBase.java:1131`). So app-facing behavior
  barely changes — this is an internal engine-room rewrite, not an
  API-breaking one.

Net effort: medium, not months — deleting a layer of indirection (the reactor)
rather than replacing one async model with another, concentrated in
`zmq.poll`, `zmq.io`, and `zmq.pipe`.

### Scala ergonomics — don't touch the core for this

Since send/recv are already blocking calls, they're trivially wrappable as-is
(`IO.blocking { socket.recv() }` in cats-effect/ZIO, or direct calls from a
Scala virtual-thread-backed executor). The ergonomics gap is about API shape,
not threading: null returns instead of `Option`, checked/unchecked exception
style, Java collection types, no `Future`/effect-typed variants, mutable
builder-style config instead of case classes. Solve this with a separate
`zeromq-scala` facade module wrapping `org.zeromq.*` — additive work, doesn't
require changing `zmq` internals, and can ship independently of the
virtual-thread migration.

**Sequencing recommendation:** do the virtual-thread reactor simplification
first (real architectural win, shrinks the codebase), then build the Scala
facade on top of the resulting simpler blocking API.

## 5. Concrete virtual-thread migration plan

**Key finding first:** `SocketBase` itself doesn't need migration. Its
`processCommands` (`SocketBase.java:1131`) drains its mailbox directly on the
calling application thread with a blocking timeout — it was never
reactor-driven. The reactor pattern lives entirely in the
**`IOThread`/`Reaper`/`StreamEngine`/listener-connecter** stack, which is the
actual scope.

### Phase 0 — Spike (few days)
Pick one transport (`zmq.io.net.tcp`), fork `StreamEngine` into a
blocking-virtual-thread variant behind a feature flag, and get one Req/Rep
round-trip test passing. Validates the approach before touching the rest.

### Phase 1 — Per-connection virtual thread replaces shared `IOThread` poller
- `IOThread` (`zmq/io/IOThread.java:14-95`) currently owns one `Poller` shared
  by every session/engine assigned to it, dispatching via `inEvent()`/mailbox
  drain. Replace the per-thread shared poller with: each
  `SessionBase`+`StreamEngine` pair gets `Thread.ofVirtual().start(...)`
  running its own blocking loop.
- `Ctx.chooseIoThread` (`Ctx.java:668`) load-balancing across a fixed
  `IOThread` pool becomes unnecessary — with virtual threads you don't need to
  bin-pack connections onto N carrier threads; just spawn one per session. The
  load-balancing mechanism (`getLoad`, `adjustLoad` in
  `PollerBase.java:100-109`) can be deleted.
- `IOObject` (`zmq/io/IOObject.java`) is the adapter every poller-driven object
  goes through (`addFd`, `setPollIn/Out`, `inEvent`/`outEvent`). Replace it
  with a class exposing blocking `readFully`/`write` calls directly against the
  `SocketChannel`, no `Handle`/poller indirection.

### Phase 2 — Rip out arm/disarm flow control in `StreamEngine`
- `StreamEngine.java` has ~10 call sites doing `setPollIn/resetPollIn/
  setPollOut/resetPollOut` (lines 312-313, 460, 468-553, 585-589, 672-822,
  1153-1243) — where readiness is toggled to implement backpressure.
- Replace with straight-line blocking code: `inEvent`/`outEvent` collapse into
  one loop per virtual thread that blocks on `channel.read()`, decodes, and
  blocks on `pipe.write()` when the pipe is full — no separate "event"
  callback needed since there's no shared reactor to hand control back to.
- Biggest behavioral change, but also a real simplification: the state
  machine that exists to survive being *interrupted* by the reactor (partial
  reads, "come back when writable") goes away.

### Phase 3 — `Pipe` backpressure becomes blocking instead of callback-driven
- Today, `SessionBase.readActivated`/`writeActivated`
  (`SessionBase.java:239-271`) call `engine.restartInput()`/`restartOutput()`
  in response to pipe-capacity changes signaled through the poller. With a
  blocking model, `Pipe.write()`/`read()` (`zmq/pipe/Pipe.java`) should just
  block the virtual thread when at HWM, rather than returning false and
  waiting for a restart callback. Needs a condition-variable-style wait added
  to `Pipe`, replacing the current non-blocking-with-signal approach.

### Phase 4 — Timers
- `PollerBase.addTimer`/`executeTimers` (`PollerBase.java:114-195`) computes
  the shared select-loop's next wakeup from a timer heap — disappears once
  there's no shared select loop per thread. Replace with
  `ScheduledExecutorService` (or a lightweight per-actor sleep) for the two
  real use sites: the linger timer in `SessionBase` (`LINGER_TIMER_ID`, lines
  55, 92, 210, 442, 464-474) and reconnect-interval timers in the connecters.

### Phase 5 — Listener/connecter accept loops (`zmq.io.net.tcp`, `ipc`)
- `AbstractSocketListener`/`AbstractSocketConnecter` currently register for
  `acceptEvent`/`connectEvent` via the shared poller. Convert to a blocking
  `accept()`/`connect()` on its own virtual thread, handing the accepted
  channel off to a newly spawned session thread. `inproc` and `pgm`/`norm`
  (datagram, no real socket) are untouched — out of scope by construction.

### Phase 6 — `Reaper` and `Mailbox`/`Signaler`
- `Reaper` (`Reaper.java:11-119`) has the identical shared-mailbox+poller shape
  as `IOThread`, but it's shutdown-path-only and low-frequency — lowest
  priority; can likely keep its current poller-based form even after
  everything else migrates, or trivially convert to a blocking mailbox-drain
  loop on its own thread.
- `Mailbox.recv` (`Mailbox.java:44-64`) already blocks via
  `signaler.waitEvent` — no change needed regardless of which actors call it.

### Phase 7 — Pinning audit and cleanup
- Only 6 `synchronized` occurrences total, confined to `SocketBase.java` and
  `Ctx.java` — audit those two files specifically; everywhere else is
  pinning-safe by default. Moot if targeting JDK 24+ (JEP 491).
- Once Phases 1–5 land, delete `zmq.poll.Poller`, `PollerBase`, `IPollEvents`,
  and `Poller.Handle` — nothing should reference them except possibly the
  retained `Reaper`.

### Phase 8 — Validation
- Run existing `zmq/socket/**`, `zmq/io/**`, `zmq/pipe/**` test suites
  unchanged (they test through `SocketBase`'s public blocking API, which
  didn't move).
- Add a throughput/latency micro-benchmark comparing old vs. new for the
  Req/Rep and Pub/Sub patterns specifically, since flow-control latency
  characteristics are the part most likely to regress or improve
  unexpectedly.

**Suggested order:** Phase 0 spike → Phase 1+2 together (inseparable — no
blocking `StreamEngine` without per-connection threads) → Phase 3 → Phase 4 →
Phase 5 → Phase 6/7 last. Phases 1–2 are the bulk of the effort; everything
after is smaller and mostly mechanical once that lands.

## 6. Is there enough test coverage to gauge correctness of a new implementation?

**Reasonably good news: 93 test files**, and the ones that matter most for
this migration are black-box — they exercise `SocketBase`'s public API, not
the reactor internals, so they'll validate the new engine without
modification.

### Solid coverage, directly relevant to what's being changed
- **Flow control / HWM**: `PubSubHwmTest`, `HighWatermarkTest` (318 lines) —
  exactly the backpressure paths Phase 3 rewrites.
- **Linger/reconnect/heartbeat edge cases**: `HeartbeatsTest` (624 lines),
  `ImmediateTest` (235 lines), `ConnectRidTest` (277 lines) — exactly the
  timer/reconnect paths Phase 4 rewrites.
- **Socket-pattern behavior**: `zmq/socket/{reqrep,pubsub,pipeline,stream}/
  *SpecTest` and `*Test` — Req/Rep, Dealer/Router (including
  `RouterMandatoryTest`, `RouterHandoverTest`, `RouterProbeTest`), Pub/Sub/
  XPub/XSub, Push/Pull.
- **Wire protocol**: `V0/V1/V2ProtocolTest`, `V1/V2DecoderTest`,
  `V1/V2EncoderTest` — validate `StreamEngine`'s framing correctness
  independent of how it's scheduled.
- **Security mechanisms**: `SecurityCurveTest`, `SecurityPlainTest`,
  `SecurityNullTest`.
- **Proxy/termination semantics**: `ProxyTerminateTest`, `ProxyTcpTest`,
  `TermEndpointIpcTest`.

Since none of these know or care whether `StreamEngine` runs on a
poller-dispatched callback or a blocking virtual thread, running this suite
unchanged before and after the migration is a legitimate correctness gate.

### Where existing tests won't help — or actively get in the way
- **`zmq/poll/PollerTest.java` and `PollerBaseTest.java`** are white-box unit
  tests against the exact classes being deleted in Phase 7 (`Poller`,
  `PollerBase`, timer heap). They test the mechanism being replaced, not
  general correctness — expect to delete them, not port them.
- **`zmq/io/StreamEngineTest.java`** is only 81 lines — thin, given
  `StreamEngine` is the file taking the biggest rewrite. Worth reading before
  starting to see exactly what it checks.
- Several tests use `@Test(timeout = 1000)`-style wall-clock timeouts
  (`PollerTest`). Virtual threads change scheduling/latency characteristics,
  so a test that passes today under the old reactor could flake under new
  timing even if logically correct — timeout-based tests are a weaker signal
  post-migration than they are today.

### Gaps to fill — nothing today checks these
- The `perf/` directory (`LocalThr`, `LocalLat`, `RemoteThr`, `InprocLat`) is a
  set of manually-run benchmark `main()`s, not asserted CI tests — good for
  eyeballing throughput/latency before vs. after, but won't fail a build on
  regression.
- Nothing tests at the concurrency scale that's the actual point of this
  migration (thousands of simultaneous connections/virtual threads) — a new
  stress test is needed, since it's the one thing existing tests structurally
  couldn't have anticipated.
- No pinning-detection test (e.g., asserting no `jdk.VirtualThreadPinned` JFR
  events fire during I/O) — worth adding as an automated gate given
  `SocketBase`/`Ctx` still have `synchronized` blocks.

### Bottom line
Treat the current suite as a real regression net for behavior (broader than
the file count alone suggests, with flow-control/reconnect/heartbeat coverage
specifically de-risking the trickiest phases), but budget for writing 2-3 new
tests — a concurrency/scale stress test and a pinning check — since those
verify the actual reason for doing this, which nothing today measures.

## 7. How would a Loom-based core interact with ZIO's own fibers?

ZIO fibers and Loom virtual threads are two *different* scheduling
abstractions that don't share a supervision tree, so this matters for the
Scala facade's design specifically.

**The core mismatch:** ZIO fibers are ZIO's own user-space scheduling
construct, multiplexed onto a fixed pool of platform threads (ZIO's "compute"
executor). A Loom-based JeroMQ, by contrast, has each connection's
`SessionBase`/`StreamEngine` doing real blocking `SocketChannel.read()`/
`write()` on its own virtual thread. Calling that blocking code directly from
inside a ZIO effect (e.g., a bare `ZIO.succeed(socket.recv())`) risks blocking
whichever compute-pool platform thread is currently running that fiber —
starving every other fiber scheduled on it, the same "don't block the event
loop" failure mode as blocking in Node.js or an actor mailbox thread.

**The correct integration pattern:** wrap every blocking JeroMQ call in
`ZIO.attemptBlocking`, which shifts it onto ZIO's separate blocking executor
rather than the compute pool. Recent ZIO 2.x lets that blocking executor be
backed by `Executors.newVirtualThreadPerTaskExecutor()`, which is the ideal
pairing here — a virtual-thread-per-blocking-call, cheap enough that pool
sizing stops being a concern the way it was when the blocking executor was a
cached native-thread pool. Get the wrapping right and the two thread models
compose cleanly: ZIO schedules fibers, and each fiber that touches JeroMQ
transparently "becomes" a virtual thread for the duration of that call.

**Two gotchas worth designing around up front:**

1. **Interruption closes the channel, not just the read.** ZIO cancels a
   fiber's blocking operation via `Thread.interrupt()` on the carrier thread.
   `SocketChannel` implements `InterruptibleChannel`, so an interrupted
   blocking read throws `ClosedByInterruptException` — but as a side effect,
   *the whole channel gets closed*, not just that one call. So
   `.timeout`/`.race`/explicit interruption on a JeroMQ-backed ZIO effect will
   tear down the connection, which JeroMQ's existing reconnect path
   (`SessionBase.engineError`, `ErrorReason.CONNECTION`) can probably absorb,
   but it means "cancel this one recv" and "kill the connection" become the
   same operation. Worth being explicit about in the facade's docs/API so it
   isn't a surprise.
2. **`FiberRef` doesn't propagate into JeroMQ's internal threads.** ZIO's
   fiber-local context (tracing, logging MDC, etc.) is carried via `FiberRef`,
   which only flows along ZIO's own fiber tree. JeroMQ's per-session virtual
   threads are plain JVM threads outside that tree, so any user callback that
   JeroMQ invokes directly from one of its own threads (rather than through a
   `ZIO.attemptBlocking` call site the user controls) won't see that context.
   This mostly matters if the Scala facade lets users register message
   handlers that JeroMQ calls back into — those need to be re-lifted into a
   ZIO fiber (running the handler back through the ZIO runtime) rather than
   executed inline on JeroMQ's thread, or fiber-local context silently
   vanishes.

**Recommendation:** design the Scala facade so the *only* boundary between the
two systems is a small number of `ZIO.attemptBlocking` call sites wrapping
JeroMQ's blocking send/recv — never let ZIO fibers call in except through that
wrapper, and never let JeroMQ call back into user code except by re-entering
the ZIO runtime explicitly. Keep the two schedulers strictly at arm's length
rather than trying to unify them; that's what makes this tractable.
