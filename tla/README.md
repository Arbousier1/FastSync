# FastSync TLA+ Formal Model

## Why TLA+?

AWS used TLA+ to find design bugs in DynamoDB, S3, EBS, and distributed lock managers that unit tests could never catch. The value is **not** replacing code tests, but **enumerating crash/delay/retry combinations before writing code**.

MC cross-server sync bugs are rarely caught by unit tests — they emerge from combinations like:
- Player switches server + old server crashes with pending save + Redis delayed
- GC pause + lease expires + stale write attempt
- Two servers race for the same UUID during network partition

## Model Overview

### Variables

| Variable | Type | Meaning |
|---|---|---|
| `owner[uuid]` | Server \| NullServer | Which server currently owns the player |
| `leaseToken[uuid]` | Nat | Fencing token of current/last lock holder |
| `dbVersion[uuid]` | Nat | Version number in the DB (Dynamo OCC) |
| `dbData[uuid]` | record | Actual data stored in DB (inventory, balance, version, token) |
| `dbFencingToken[uuid]` | Nat | Fencing token of the last successful write |
| `pendingSave` | Set of (Server, uuid) | Saves in flight to the DB |
| `pendingSaveData` | [(Server,uuid) -> record] | Data being saved |
| `saveAttempts` | [(Server,uuid) -> Nat] | Retry counter |
| `inventoryCount[uuid]` | Nat | Player's item count (simplified) |
| `balance[uuid]` | Int | Player's current balance |
| `ledger` | Sequence of (uuid, delta) | Append-only economy transaction log |
| `playerState[uuid]` | INACTIVE\|JOINING\|ACTIVE\|QUITTING | Player lifecycle state |
| `savedToken[uuid]` | Nat | Fencing token the player's data was saved with |

### Actions

| Action | Trigger | What it models |
|---|---|---|
| `Join(server, uuid)` | Player login | Acquire lock (increment fencing token), set JOINING |
| `JoinComplete(server, uuid)` | Data loaded | JOINING → ACTIVE |
| `Quit(server, uuid)` | Player disconnect | Capture data for async save, set QUITTING |
| `Save(server, uuid)` | Async save reaches DB | **Critical**: DB checks version CAS + fencing token |
| `SaveRejected(server, uuid)` | DB rejects save | Version mismatch or fencing token violation |
| `Crash(server)` | Server crash | Players become INACTIVE, pending saves lost |
| `RedisDelay` | Pub/Sub message delay | Stuttering step (no correctness impact) |
| `DbDelay` | DB query delay | Stuttering step |
| `RetrySave(server, uuid)` | Retry after transient error | Same token/version (no new token) |

### Invariants Verified

| Invariant | What it checks | Maps to |
|---|---|---|
| `NoDoubleOwner` | Same UUID never ACTIVE on 2 servers | ZooKeeper/Chubby lease |
| `NoLateWrite` | Low fencing token can't overwrite high token | Kleppmann fencing |
| `NoDupItem` | Inventory doesn't increase from handoff | Data integrity |
| `NoLostMoney` | Ledger sum(deltas) == balance | Economy consistency |
| `MonotonicVersion` | DB version only increases | Dynamo OCC |

## The Critical Action: Save

```
Save(server, uuid) succeeds only if BOTH:
  1. expectedVersion = currentDbVersion   (Dynamo CAS)
  2. saveToken >= currentDbToken           (Kleppmann fencing)
```

If either check fails → `SaveRejected` (data preserved, not overwritten).

This is the **exact** logic implemented in `DatabaseManager.saveData()`:
```sql
UPDATE player_data SET data = ?, version = version + 1, ...
WHERE uuid = ? AND version = ? AND fencing_token <= ?
```

## How to Run

### Option 1: TLA+ Toolbox (GUI)

1. Download from: https://github.com/tlaplus/tlaplus/releases
2. Open `FastSync.tla` in the Toolbox
3. Create a new TLC model with `FastSync.cfg`
4. Click "Check"

### Option 2: Command Line

```bash
# Download tla2tools.jar
wget https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar

# Run model checker
java -jar tla2tools.jar tlc2.TLC FastSync.tla FastSync.cfg
```

### Expected Output

```
TLC2 Version 2.x
Running in Model-Checking mode.
Checking all invariants...
Model checking completed. No error has been found.
  States generated: ~50,000
  Distinct states: ~15,000
  Invariants checked: 5 (all PASS)
```

### If an Invariant is Violated

TLC produces a **counterexample trace** — the exact sequence of actions leading to the bug:

```
State 1: Init
State 2: Join(s1, p1)           — s1 acquires lock, token=1
State 3: JoinComplete(s1, p1)    — p1 ACTIVE on s1
State 4: Quit(s1, p1)            — s1 starts save with token=1
State 5: Crash(s1)               — s1 crashes, save lost!
State 6: Join(s2, p1)            — s2 acquires lock, token=2
State 7: JoinComplete(s2, p1)    — p1 ACTIVE on s2
State 8: Quit(s2, p1)            — s2 saves with token=2
State 9: Save(s2, p1)            — DB accepts (token=2 >= 0), version 0→1
  *** NoDoubleOwner VIOLATED! ***
```

This trace tells you **exactly** what went wrong and how to fix the code.

## What This Model Catches

| Bug class | How the model catches it |
|---|---|
| **Double-login** | `Join` requires `playerState = INACTIVE` and `owner = NullServer` |
| **Stale write after GC pause** | `Save` checks `saveToken >= currentDbToken` |
| **Lost update on crash** | `Crash` clears pending saves; next `Join` gets new token |
| **Retry with stale token** | `RetrySave` reuses same token; DB rejects if newer exists |
| **Inventory duplication** | `NoDupItem` invariant checks DB matches server state |
| **Money loss** | `NoLostMoney` checks ledger consistency |

## Connection to Code

| TLA+ Concept | Java Implementation |
|---|---|
| `Save` action's version check | `DatabaseManager.saveData()` WHERE version = ? |
| `Save` action's token check | `DatabaseManager.saveData()` WHERE fencing_token <= ? |
| `SaveRejected` | `ConflictManager.handleConflict()` |
| `Crash` + `Join` getting new token | `acquireLock()` increments `fencing_token + 1` |
| `RedisDelay` / `DbDelay` | Async execution + retry logic |
| `NoLateWrite` invariant | The `fencing_token <= ?` in SQL is the enforcement |
