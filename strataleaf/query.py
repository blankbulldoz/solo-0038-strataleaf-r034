from bisect import bisect_right
from dataclasses import dataclass
from .codec import MISSING, digest, dumps, field, loads
from .cursors import decode_cursor, encode_cursor
from .errors import InvalidCursor, InvalidDocument, NotFound
from .indexes import equality_candidates
from .mvcc import checked_revision, record, visible_rows, latest_row
from .predicates import equality_terms, matches, validate_predicate

MAX_ORDER_FIELDS = 16
CURSOR_VERSION = 2


@dataclass(frozen=True)
class Page:
    records: list
    revision: int
    cursor: str | None
    plan: dict


class _Descending:
    """Invert a sort token without changing the stable key tie-breaker."""
    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return isinstance(other, _Descending) and self.value == other.value

    def __lt__(self, other):
        return other.value < self.value


def normalize_order_by(order_by):
    """Return a canonical list of {field, direction} entries.

    Accepts a field name, one specification object, or a sequence of both.
    The empty sequence means the historical key-ascending order.
    """
    if order_by is None:
        return []
    if isinstance(order_by, (str, dict)):
        entries = [order_by]
    elif isinstance(order_by, (list, tuple)):
        entries = list(order_by)
    else:
        raise InvalidDocument("order_by must be a field name, an object, or a sequence of those")
    if len(entries) > MAX_ORDER_FIELDS:
        raise InvalidDocument(f"order_by accepts at most {MAX_ORDER_FIELDS} fields")
    spec, seen = [], set()
    for entry in entries:
        if isinstance(entry, str):
            path, direction = entry, "asc"
        elif isinstance(entry, dict) and set(entry) <= {"field", "direction"} and "field" in entry:
            path, direction = entry["field"], entry.get("direction", "asc")
        else:
            raise InvalidDocument("each order_by entry must be a field name or {field, direction}")
        field({}, path)
        if direction not in ("asc", "desc"):
            raise InvalidDocument("order_by direction must be asc or desc")
        if path in seen:
            raise InvalidDocument("order_by contains a duplicate field: " + path)
        seen.add(path)
        spec.append({"field": path, "direction": direction})
    return spec


def _value_token(value):
    """Build a total-order token for any JSON value.

    The leading rank makes mixed types comparable:
    missing < null < boolean < number < string < array < object.
    Within a type the payload is compared directly; numbers use numeric
    (not textual) order and arrays compare lexicographically. Objects use
    their canonical JSON text so the order is deterministic.
    """
    if value is MISSING:
        return (0, ())
    if value is None:
        return (1, ())
    if isinstance(value, bool):
        return (2, (1 if value else 0,))
    if isinstance(value, (int, float)):
        return (3, (value,))
    if isinstance(value, str):
        return (4, (value,))
    if isinstance(value, list):
        return (5, tuple(_value_token(item) for item in value))
    return (6, (dumps(value),))


def _row_order_key(document, spec, key):
    parts = []
    for entry in spec:
        token = _value_token(field(document, entry["field"]))
        parts.append(_Descending(token) if entry["direction"] == "desc" else token)
    # Document keys are the final, always-ascending tie-breaker. Without it
    # two documents sharing every sort value could be skipped or repeated
    # at a page boundary.
    parts.append(key)
    return tuple(parts)


class QueryEngine:
    def __init__(self, store):
        self.store = store

    def select(self, collection, predicate=None, limit=100, cursor=None, revision=None,
               force_scan=False, order_by=None, sort=None):
        validate_predicate(predicate)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise InvalidDocument("page size must be between 1 and 1000")
        if order_by is not None and sort is not None:
            raise InvalidDocument("specify order_by or sort, not both")
        spec = normalize_order_by(order_by if order_by is not None else sort)
        with self.store.read() as connection:
            if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
                raise NotFound("unknown collection")
            # Key-ascending queries keep the original fingerprint and cursor
            # shape so cursors issued before field ordering existed remain
            # usable. Field-ordered cursors bind the complete order spec.
            if spec:
                fingerprint = digest({"collection": collection, "predicate": predicate,
                                      "order_by": spec, "cursor_version": CURSOR_VERSION})
            else:
                fingerprint = digest({"collection": collection, "predicate": predicate})
            after = None
            if cursor is not None:
                payload = decode_cursor(self.store, cursor)
                if spec:
                    if (payload.get("cursor_version") != CURSOR_VERSION
                            or payload.get("order_by") != spec):
                        raise InvalidCursor("cursor does not use this field ordering")
                elif "order_by" in payload or "cursor_version" in payload:
                    raise InvalidCursor("cursor belongs to a field-ordered query")
                if payload.get("query") != fingerprint or (revision is not None and revision != payload.get("revision")):
                    raise InvalidCursor("cursor does not belong to this query/read revision")
                revision, after = payload.get("revision"), payload.get("after")
                if not isinstance(after, str):
                    raise InvalidCursor("cursor has no key boundary")
            revision = checked_revision(self.store, revision)
            head = int(self.store.get("head"))
            plan = {"access": "version_scan", "index": None,
                    "ordering": "fields" if spec else "key",
                    "order_by": spec, "tie_break": "key",
                    "revision_path": "current" if revision == head else "historical"}
            candidates = None
            # Materialized indexes describe the current head only. Historical
            # reads (and explicit force_scan) always use a version scan.
            if not force_scan and revision == head:
                for path, value in equality_terms(predicate):
                    indexed = equality_candidates(connection, collection, path, value)
                    if indexed is not None:
                        index_name, candidates = indexed
                        plan = {**plan, "access": "equality_index", "index": index_name}
                        break
            if candidates is None:
                rows = visible_rows(connection, collection, revision)
            else:
                rows = [latest_row(connection, collection, key, revision) for key in sorted(candidates)]

            if not spec:
                output = []
                examined = 0
                for row in rows:
                    if row is None or row["body"] is None or (after is not None and row["key"] <= after):
                        continue
                    examined += 1
                    item = record(row)
                    if matches(item.document, predicate):
                        output.append(item)
                        if len(output) > limit:
                            break
                continuation = None
                if len(output) > limit:
                    output = output[:limit]
                    continuation = encode_cursor(self.store, {"database": self.store.get("database_id"),
                        "query": fingerprint, "revision": revision, "after": output[-1].key})
                return Page(output, revision, continuation, {**plan, "examined": examined})

            # Field ordering is evaluated in memory against the fixed
            # revision. The ordering tuple ends with the document key, so
            # duplicate sort values still have one stable position.
            entries = []
            for row in rows:
                if row is None or row["body"] is None:
                    continue
                document = loads(row["body"])
                entries.append((_row_order_key(document, spec, row["key"]), row, document))
            entries.sort(key=lambda entry: entry[0])
            start = 0
            if after is not None:
                boundary_row = latest_row(connection, collection, after, revision)
                if boundary_row is None or boundary_row["body"] is None:
                    raise InvalidCursor("cursor boundary is not visible at the read revision")
                boundary = _row_order_key(loads(boundary_row["body"]), spec, after)
                start = bisect_right([entry[0] for entry in entries], boundary)
            output = []
            examined = 0
            for _, row, document in entries[start:]:
                examined += 1
                if matches(document, predicate):
                    output.append(record(row))
                    if len(output) > limit:
                        break
            continuation = None
            if len(output) > limit:
                output = output[:limit]
                continuation = encode_cursor(self.store, {"database": self.store.get("database_id"),
                    "query": fingerprint, "revision": revision, "after": output[-1].key,
                    "order_by": spec, "cursor_version": CURSOR_VERSION})
            return Page(output, revision, continuation, {**plan, "examined": examined})
