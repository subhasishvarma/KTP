# KTP — Reliable Transport Protocol over UDP

> A from-scratch reliable transport layer built in C, implementing Go-Back-N sliding window over UDP via a POSIX shared-memory daemon — designed to simulate and measure packet loss behavior across varying drop probabilities.

---

## Overview

KTP (KTP Transport Protocol) provides reliable, ordered message delivery on top of raw UDP sockets. It mirrors the structure of the kernel's socket API — `k_socket`, `k_bind`, `k_sendto`, `k_recvfrom`, `k_close` — so user programs interact with it just like standard sockets, with reliability handled transparently underneath.

The system consists of three components:

- **`libksocket.a`** — the user-facing library (API + shared memory client)
- **`initksocket`** — the background daemon running Threads R, S, and GC
- **`user1` / `user2`** — sender and receiver test programs

---

## Architecture

```
user1 / user2
    │  k_sendto / k_recvfrom
    ▼
libksocket.a  ──── shared memory (SM[]) ────  initksocket daemon
                                                  ├── Thread R  (receive + ACK)
                                                  ├── Thread S  (send + retransmit)
                                                  └── Thread GC (dead-owner cleanup)
```

All socket state lives in a POSIX shared memory segment (`SM[]`) with up to 100 slots. The user library reads and writes this segment directly; the daemon owns all actual UDP I/O.

---

## Protocol Design

| Parameter | Value |
|-----------|-------|
| Message size | 512 bytes (fixed) |
| Window size | 10 slots (send + receive) |
| Retransmission timeout | 5 seconds |
| Retransmission policy | Go-Back-N |
| Sequence numbers | 8-bit, wraps at 256 |
| Drop simulation | Configurable probability `p` in `ksocket.h` |

### Wire Format (`KTPHeader`)

```c
char     pkt_type;   // 'D' = data, 'A' = ACK
uint8_t  seq;        // sequence number (starts at 1)
int      rwnd_sz;    // piggybacked receiver window size (ACKs only)
uint16_t msg_len;    // actual payload length in this slot
```

### Thread Responsibilities

**Thread R** — runs `select()` on all active UDP sockets with a T-second timeout:
- Simulates packet loss via `dropMessage(p)` on every incoming packet
- For data packets: buffers in-order messages, sends cumulative ACKs with piggybacked `rwnd`
- For ACK packets: advances the sender window, updates `win_size` from `rwnd_sz`
- On timeout: sends beacon ACKs if the receiver buffer was full (`nospace=true`), preventing sender deadlock

**Thread S** — wakes every T/2 seconds:
- Transmits new messages within the current window
- Detects retransmission timeouts and resets `next_seq = base_seq` (Go-Back-N)

**Thread GC** — wakes every 10 seconds:
- Detects dead owner processes via `kill(pid, 0)`
- Reclaims their shared memory slots

---

## Build & Run

**Requirements:** GCC, POSIX threads, POSIX shared memory (`-lrt -lpthread`)

```bash
# Step 1: build the daemon and library
make -f Makefile_init    # produces initksocket
make -f Makefile_lib     # produces libksocket.a

# Step 2: build user programs
make -f Makefile_users   # produces user1, user2

# Step 3: run
./initksocket                                          # Terminal 1
./user2 127.0.0.1 8081 127.0.0.1 8080 output.txt     # Terminal 2
./user1 127.0.0.1 8080 127.0.0.1 8081 input.txt      # Terminal 3
```

To change the drop probability, edit `DROP_PROB` in `ksocket.h` and rebuild.

---

## Transmission Statistics

Benchmarked over a 100 KB+ file (197 messages of 512 bytes each) across 10 drop probabilities:

| p    | Messages | Total Transmissions | Avg Tx / Msg |
|------|----------|---------------------|--------------|
| 0.05 | 197      | 218                 | 1.107        |
| 0.10 | 197      | 234                 | 1.188        |
| 0.15 | 197      | 286                 | 1.452        |
| 0.20 | 197      | 360                 | 1.827        |
| 0.25 | 197      | 386                 | 1.959        |
| 0.30 | 197      | 505                 | 2.563        |
| 0.35 | 197      | 568                 | 2.883        |
| 0.40 | 197      | 622                 | 3.157        |
| 0.45 | 197      | 683                 | 3.467        |
| 0.50 | 197      | 707                 | 3.589        |

At p=0.05, retransmissions add only ~10% overhead. At p=0.50, each message requires ~3.6 transmissions on average — consistent with Go-Back-N behavior under high loss.

---

## File Structure

```
ksocket.h        — shared data structures (KTPHeader, KTPSocket, windows, SM[])
ksocket.c        — user library: k_socket, k_bind, k_sendto, k_recvfrom, k_close
initksocket.c    — daemon: Thread R, Thread S, Thread GC, shared memory setup
user1.c          — sender: reads input file, transmits over KTP
user2.c          — receiver: receives over KTP, writes to output file
Makefile_init    — builds initksocket
Makefile_lib     — builds libksocket.a
Makefile_users   — builds user1, user2
documentation.txt — data structures, function specs, benchmark results
```

---

## Course Context

Mini Project 1 for **CS39006 — Computer Networks Laboratory** at IIT Kharagpur.
Submitted by M Subhasish Varma (23CS30034).
