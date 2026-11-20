import json
import sys
import tempfile
from pathlib import Path
from strataleaf import Database, Archives, Conflict, InvalidDocument, InvalidCursor, ReadOnly, SnapshotTooOld, ReplicationError

SCHEMA = {"fields": {"region": "string", "sequence": "integer", "code": "string"}, "required": ["region", "sequence"]}


def seed(db):
    db.catalog.create("segments", SCHEMA)
    batch = db.batch("initial_segments")
    for index in range(6):
        batch.put("segments", f"seg_{index:02}", {"region": "east" if index % 2 == 0 else "west", "sequence": index, "code": "c" + str(index)})
    return batch.commit().revision


def rejected(call, error):
    try:
        call()
    except error as exc:
        return {"error": type(exc).__name__, "message": str(exc)}
    raise RuntimeError("operation unexpectedly succeeded")


def run(flow):
    with tempfile.TemporaryDirectory(prefix="strataleaf-example-") as directory:
        folder = Path(directory)
        with Database(folder / "source.db") as db:
            first = seed(db)
            if flow == "catalog":
                output = {"collections": db.catalog.list(), "duplicate": rejected(lambda: db.catalog.create("segments"), Conflict)}
            elif flow == "schema":
                updated = {**SCHEMA, "fields": {**SCHEMA["fields"], "note": "string"}}
                generation = db.catalog.evolve("segments", updated, 1)
                failure = rejected(lambda: db.catalog.evolve("segments", {**updated, "required": ["note"]}, generation), InvalidDocument)
                output = {"generation": generation, "after_rejection": db.catalog.describe("segments"), "incompatible": failure}
            elif flow == "commit":
                db.catalog.create("marks")
                result = db.batch("dual_write").put("segments", "seg_00", {"region": "south", "sequence": 7}, expected=first).put("marks", "m0", {"segment": "seg_00"}).commit()
                replay = db.batch("dual_write").put("segments", "seg_00", {"region": "south", "sequence": 7}, expected=first).put("marks", "m0", {"segment": "seg_00"}).commit()
                conflict = rejected(lambda: db.batch().put("marks", "discard", {}).put("segments", "seg_01", {"region": "x", "sequence": 1}, expected=0).commit(), Conflict)
                if db.get("marks", "discard") is not None or not replay.replayed:
                    raise RuntimeError("atomicity or replay observation failed")
                output = {"commit": result.__dict__, "replay": replay.__dict__, "conflict": conflict}
            elif flow == "snapshot":
                db.batch().delete("segments", "seg_00", expected=first).commit()
                output = {"past": db.get("segments", "seg_00", first).__dict__, "present": db.get("segments", "seg_00"),
                          "future": rejected(lambda: db.get("segments", "seg_00", db.revision + 1), InvalidDocument)}
            elif flow == "query":
                query = {"field": "sequence", "op": "ge", "value": 0}
                page = db.query("segments", query, limit=2)
                db.batch().delete("segments", "seg_02").put("segments", "seg_07", {"region": "west", "sequence": 7}).commit()
                keys = [row.key for row in page.records]
                cursor = page.cursor
                while cursor:
                    next_page = db.query("segments", query, limit=2, cursor=cursor)
                    keys.extend(row.key for row in next_page.records)
                    cursor = next_page.cursor
                if keys != [f"seg_{i:02}" for i in range(6)]:
                    raise RuntimeError("read revision changed during pagination")
                if page.plan["ordering"] != "key":
                    raise RuntimeError("historical key ordering not used by default")

                # Field ordering: duplicate values, missing/null/mixed types and
                # document key as the final tie-breaker.
                db.catalog.create("ranked", {})
                db.batch().put("ranked", "a", {"group": "x", "weight": 2}).put(
                    "ranked", "b", {"group": "x", "weight": 2}).put(
                    "ranked", "c", {"group": "x", "weight": 1}).put(
                    "ranked", "d", {"group": "x"}).put(
                    "ranked", "e", {"group": "x", "weight": None}).put(
                    "ranked", "f", {"group": "x", "weight": "2"}).put(
                    "ranked", "g", {"group": "y", "weight": 2}).commit()
                order = [{"field": "group", "direction": "asc"}, {"field": "weight", "direction": "desc"}]
                ordered = db.query("ranked", order_by=order, limit=3)
                ordered_cursor, seen = ordered.cursor, [row.key for row in ordered.records]
                while ordered_cursor:
                    more = db.query("ranked", order_by=order, limit=3, cursor=ordered_cursor)
                    seen.extend(row.key for row in more.records)
                    ordered_cursor = more.cursor
                # group asc (x < y); weight desc reverses the full type order, so
                # string "2" > number 2 > number 1 > null > missing. Equal sort
                # values fall back to key asc ("a" before "b").
                if seen != ["f", "a", "b", "c", "e", "d", "g"]:
                    raise RuntimeError("field ordering is not stable: " + repr(seen))
                # Writes during pagination neither leak in nor drop rows because
                # every page reads the revision bound to the cursor.
                first_page = db.query("ranked", order_by=order, limit=2)
                db.batch().put("ranked", "a", {"group": "z", "weight": 9}).delete("ranked", "g").commit()
                rest = []
                cursor = first_page.cursor
                while cursor:
                    next_page = db.query("ranked", order_by=order, limit=2, cursor=cursor)
                    rest.extend(row.key for row in next_page.records)
                    cursor = next_page.cursor
                if [row.key for row in first_page.records] + rest != seen:
                    raise RuntimeError("field ordering changed while writes happened")
                shifted = rejected(lambda: db.query("ranked", order_by="group", cursor=first_page.cursor), InvalidCursor)
                historical = db.query("ranked", order_by=order, revision=page.revision)
                output = {"revision": page.revision, "keys": keys, "ordering": ordered.plan,
                          "ordered_keys": seen, "after_writes": [row.key for row in first_page.records] + rest,
                          "foreign_query": rejected(lambda: db.query("segments", cursor=page.cursor), InvalidCursor),
                          "changed_order": shifted, "historical_path": historical.plan["revision_path"],
                          "legacy_cursor": page.plan["ordering"]}
            elif flow == "index":
                db.indexes.create("segments", "by_region", "region")
                db.indexes.create("segments", "unique_code", "code", unique=True)
                predicate = {"field": "region", "op": "eq", "value": "east"}
                indexed = db.query("segments", predicate)
                scanned = db.query("segments", predicate, force_scan=True)
                if indexed.records != scanned.records:
                    raise RuntimeError("index and scan differ")
                collision = rejected(lambda: db.batch().put("segments", "new", {"region": "north", "sequence": 8, "code": "c0"}).commit(), Conflict)
                output = {"plan": indexed.plan, "keys": [row.key for row in indexed.records], "collision": collision, "integrity": db.diagnostics.inspect(True)}
            elif flow == "feed":
                db.feed.register("mirror", 0)
                db.batch().delete("segments", "seg_00").commit()
                commits = db.feed.read(0, limit=1)
                db.feed.acknowledge("mirror", commits[-1]["revision"])
                output = {"complete_commit": commits, "ack": db.feed.position("mirror"),
                          "backward": rejected(lambda: db.feed.acknowledge("mirror", 0), Conflict)}
            elif flow == "backup":
                backup = db.archives.backup(folder / "backup.db")
                db.batch().delete("segments", "seg_00").commit()
                bad = rejected(lambda: Archives.restore(folder / "backup.db", folder / "invalid.db", "wrong"), Exception)
                Archives.restore(folder / "backup.db", folder / "restored.db", backup["sha256"])
                with Database(folder / "restored.db") as restored:
                    output = {"backup": backup, "restored": restored.diagnostics.fingerprint(), "bad_checksum": bad}
                    if restored.revision != first:
                        raise RuntimeError("backup read boundary changed")
            elif flow == "replica":
                db.archives.backup(folder / "base.db")
                Archives.restore(folder / "base.db", folder / "replica.db", follower=True)
                db.batch().put("segments", "seg_00", {"region": "north", "sequence": 19}).commit()
                db.batch().delete("segments", "seg_01").commit()
                db.indexes.create("segments", "by_region", "region")
                with Database(folder / "replica.db") as replica:
                    part = db.replication.export(replica.revision, limit=1)
                    applied = replica.replication.apply(part)
                    tail = db.replication.export(replica.revision)
                    replica.replication.apply(tail)
                    repeat = replica.replication.apply(tail)
                    readonly = rejected(lambda: replica.batch().delete("segments", "seg_00").commit(), ReadOnly)
                    if replica.diagnostics.fingerprint() != db.diagnostics.fingerprint() or not replica.diagnostics.inspect(True)["ok"]:
                        raise RuntimeError("replica differs from source")
                    output = {"first_packet": applied, "repeat": repeat, "readonly": readonly, "fingerprint": replica.diagnostics.fingerprint()}
            elif flow == "compact":
                db.retention.pin("reader", first)
                db.feed.register("slow_copy", first)
                db.batch().put("segments", "seg_00", {"region": "north", "sequence": 8}).commit()
                blocked = db.retention.compact(db.revision)
                old = db.get("segments", "seg_00", first)
                db.retention.unpin("reader")
                db.feed.acknowledge("slow_copy", db.revision)
                advanced = db.retention.compact(db.revision)
                expired = rejected(lambda: db.get("segments", "seg_00", first), SnapshotTooOld)
                replay = db.batch("initial_segments")
                for index in range(6):
                    replay.put("segments", f"seg_{index:02}", {"region": "east" if index % 2 == 0 else "west", "sequence": index, "code": "c" + str(index)})
                output = {"blocked": blocked, "old_value": old.document, "advanced": advanced, "expired_read": expired, "replay_after_compaction": replay.commit().__dict__}
            elif flow == "expire":
                written = db.batch().put("segments", "timed", {"region": "north", "sequence": 10}, expires_at=100).commit()
                before = db.get("segments", "timed")
                swept = db.retention.sweep(100)
                if db.get("segments", "timed") is not None or db.get("segments", "timed", written.revision) is None:
                    raise RuntimeError("expiry did not preserve historical visibility")
                output = {"before": before.__dict__, "sweep": swept, "feed": db.feed.read(written.revision), "idle": db.retention.sweep(100)}
            elif flow == "aggregate":
                output = db.aggregates.group("segments", "region", "sequence", revision=first)
                if sum(item["count"] for item in output["groups"]) != 6:
                    raise RuntimeError("aggregation lost visible documents")
                output["invalid"] = rejected(lambda: db.aggregates.group("unknown", "region"), Exception)
            elif flow == "patch":
                result = db.patch("segments", "seg_00", {"code": None, "detail": {"block": 4}}, expected=first)
                output = {"patch": result.__dict__, "document": db.get("segments", "seg_00").document,
                          "stale": rejected(lambda: db.patch("segments", "seg_00", {"sequence": 8}, expected=first), Conflict)}
            elif flow == "inspect":
                db.indexes.create("segments", "by_code", "code")
                output = {"inspection": db.diagnostics.inspect(deep=True), "fingerprint": db.diagnostics.fingerprint(), "checkpoint": db.checkpoint()}
                if not output["inspection"]["ok"]:
                    raise RuntimeError("integrity inspection failed")
            elif flow == "transfer":
                exported = db.transfer.export_lines("segments")
                db.catalog.create("copy_segments", SCHEMA)
                imported = db.transfer.import_lines("copy_segments", exported["lines"])
                output = {"revision": exported["revision"], "imported": imported.__dict__,
                          "duplicate": rejected(lambda: db.transfer.import_lines("copy_segments", exported["lines"]), Conflict)}
                if len(db.query("copy_segments").records) != 6:
                    raise RuntimeError("logical import differs")
            else:
                raise ValueError("unknown example workflow: " + flow)
            print(json.dumps({"workflow": flow, "result": output}, ensure_ascii=False, sort_keys=True, default=str))


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "commit")
