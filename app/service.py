"""Domain logic for alarm intake, confirmation and escalation.

Design invariants
-----------------
1. Submissions are idempotent by ``alarm_no`` + content fingerprint. An exact
   retry returns the existing alarm; a differing body is rejected with 409.
2. A notification is persisted (with its full POST payload) *before* any HTTP
   call. The notification number is the gateway idempotency key, so re-posting
   after a crash can never page twice.
3. The escalation clock starts only when the gateway has confirmed that the
   primary notification was accepted. An unproven outcome is recorded as
   ``unknown`` and re-checked through the gateway lookup endpoint; it is never
   treated as delivered and never starts the timer.
4. Confirmation and escalation are serialised by SQLite write transactions and
   arbitrated by conditional UPDATEs. If the confirmation wins, no backup
   notification is created and a queued-but-undispatched backup is cancelled.
   If escalation wins, the confirmation is recorded but creates no new
   delivery; an already in-flight backup is reconciled and reported truthfully.
5. At most one backup notification exists per alarm (unique index), so a
   deadline can only ever enqueue a single backup delivery.
"""

from __future__ import annotations

import json
import secrets
from typing import Any

from .db import Database, iso_from_real, parse_ts, utc_now
from .view import build_alarm_view


class ConflictError(Exception):
    """Same alarm number, different content."""


class NotFoundError(Exception):
    pass


class InvalidStateError(Exception):
    pass


def _new_notification_no(alarm_no: str, role: str) -> str:
    # The random suffix makes the number globally unique. It is generated once
    # and persisted, so every retry of this notification uses the same number.
    return f"N-{alarm_no}-{role}-{secrets.token_hex(6)}"


def _cabinet_of(metadata: dict[str, Any]) -> str | None:
    """Normalise ``metadata.cabinet`` to a non-empty string, or None.

    Silence matching is exact-string based, so scalars are coerced the same way
    at submission time and at silence-lookup time.
    """
    value = metadata.get("cabinet")
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (str, int, float)):
        text = str(value).strip()
        return text or None
    return None


class AlarmService:
    def __init__(self, db: Database):
        self.db = db

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _event(conn, alarm_no: str, kind: str, detail: str | None = None) -> None:
        # Same collapse rule as notification history: a repeated unknown-receipt
        # event on consecutive ticks is recorded once. A different kind in
        # between starts a new entry, so state changes remain fully ordered.
        previous = conn.execute(
            "SELECT kind, detail FROM events WHERE alarm_no = ? ORDER BY id DESC LIMIT 1",
            (alarm_no,),
        ).fetchone()
        if previous is not None and previous["kind"] == kind and previous["detail"] == detail:
            conn.execute(
                "UPDATE events SET at = ? WHERE id = "
                "(SELECT id FROM events WHERE alarm_no = ? ORDER BY id DESC LIMIT 1)",
                (utc_now(), alarm_no),
            )
            return
        conn.execute(
            "INSERT INTO events (alarm_no, at, kind, detail) VALUES (?, ?, ?, ?)",
            (alarm_no, utc_now(), kind, detail),
        )

    @staticmethod
    def _append_history(history_json: str | None, kind: str, detail: str | None) -> str:
        try:
            history = json.loads(history_json) if history_json else []
        except ValueError:
            history = []
        # Collapse consecutive identical entries: an unresolved notification is
        # re-checked on every tick, and the history must not grow without bound.
        entry = {"at": utc_now(), "kind": kind, "detail": detail}
        if history and history[-1].get("kind") == kind and history[-1].get("detail") == detail:
            history[-1]["at"] = entry["at"]
        else:
            history.append(entry)
        return json.dumps(history, ensure_ascii=False)

    @staticmethod
    def _notification_row(
        *,
        alarm_no: str,
        role: str,
        contact: dict[str, Any],
        content_hash: str,
        payload: dict[str, Any],
        created_by: str,
    ) -> dict[str, Any]:
        now = utc_now()
        now_real = parse_ts(now)
        return {
            "alarm_no": alarm_no,
            "notification_no": _new_notification_no(alarm_no, role),
            "role": role,
            "contact_json": json.dumps(contact, ensure_ascii=False, sort_keys=True),
            "content_hash": content_hash,
            "payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
            "created_at": now,
            "created_real": now_real,
            "state": "pending",
            "attempts": 0,
            "created_by": created_by,
            "history_json": json.dumps(
                [{"at": now, "kind": "enqueued", "detail": f"created by {created_by}"}],
                ensure_ascii=False,
            ),
            "next_attempt_at": now_real,
        }

    def _insert_notification(self, conn, row: dict[str, Any]) -> None:
        columns = ", ".join(row.keys())
        placeholders = ", ".join("?" for _ in row)
        conn.execute(
            f"INSERT INTO notifications ({columns}) VALUES ({placeholders})",
            tuple(row.values()),
        )

    @staticmethod
    def _deadline_seconds(conn, alarm_no: str) -> int:
        row = conn.execute(
            "SELECT ack_deadline_seconds FROM alarms WHERE alarm_no = ?", (alarm_no,)
        ).fetchone()
        return int(row["ack_deadline_seconds"])

    # ------------------------------------------------------------------ intake

    def submit_alarm(self, payload) -> tuple[dict[str, Any], bool]:
        """Return (alarm view, deduplicated)."""
        content_hash = payload.content_hash()
        now = utc_now()
        now_real = parse_ts(now)

        with self.db.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM alarms WHERE alarm_no = ?", (payload.alarm_no,)
            ).fetchone()
            if existing is not None:
                if existing["content_hash"] != content_hash:
                    raise ConflictError(
                        f"alarm {payload.alarm_no!r} already exists with different content"
                    )
                return self._snapshot(conn, payload.alarm_no, now_real), True

            conn.execute(
                """
                INSERT INTO alarms (
                    alarm_no, content_hash, payload_json, primary_contact,
                    backup_contact, ack_deadline_seconds, cabinet, created_at, created_real
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.alarm_no,
                    content_hash,
                    json.dumps(payload.model_dump(mode="json"), ensure_ascii=False, sort_keys=True),
                    json.dumps(payload.primary_contact.model_dump(mode="json"), ensure_ascii=False, sort_keys=True),
                    json.dumps(payload.backup_contact.model_dump(mode="json"), ensure_ascii=False, sort_keys=True),
                    payload.ack_deadline_seconds,
                    _cabinet_of(payload.metadata),
                    now,
                    now_real,
                ),
            )

            primary = self._notification_row(
                alarm_no=payload.alarm_no,
                role="primary",
                contact=payload.primary_contact.model_dump(mode="json"),
                content_hash=content_hash,
                payload={
                    "content": payload.content,
                    "metadata": payload.metadata,
                    "ack_deadline_seconds": payload.ack_deadline_seconds,
                },
                created_by="initial",
            )
            self._insert_notification(conn, primary)
            self._event(
                conn,
                payload.alarm_no,
                "submitted",
                f"primary notification {primary['notification_no']}",
            )
            return self._snapshot(conn, payload.alarm_no, now_real), False

    # ----------------------------------------------------------------- confirm

    def _cancel_pending(self, conn, alarm_no: str, *, detail: str, history: str) -> list[str]:
        """Cancel every notification of this alarm that was never handed to the
        gateway, and return the cancelled roles.

        Only ``pending`` rows qualify: anything already dispatched (in flight or
        with an ambiguous receipt) is left for gateway reconciliation — the API
        never claims to have cancelled a notification the gateway may already
        have accepted. The conditional UPDATE serialises the cancel against a
        concurrent delivery claim.
        """
        rows = conn.execute(
            "SELECT id, role, history_json FROM notifications "
            " WHERE alarm_no = ? AND state = 'pending'",
            (alarm_no,),
        ).fetchall()
        cancelled: list[str] = []
        for row in rows:
            updated = conn.execute(
                """
                UPDATE notifications
                   SET state = 'cancelled', detail = ?, history_json = ?,
                       next_attempt_at = NULL, lease_until = NULL
                 WHERE id = ? AND state = 'pending'
                """,
                (
                    detail,
                    self._append_history(row["history_json"], "cancelled", history),
                    row["id"],
                ),
            ).rowcount
            if updated:
                cancelled.append(row["role"])
        return cancelled

    def confirm(self, alarm_no: str, confirmed_by: str, note: str | None) -> dict[str, Any]:
        """Confirm an alarm and atomically prevent any new backup delivery."""
        now = utc_now()
        now_real = parse_ts(now)
        with self.db.transaction() as conn:
            alarm = conn.execute("SELECT * FROM alarms WHERE alarm_no = ?", (alarm_no,)).fetchone()
            if alarm is None:
                raise NotFoundError(alarm_no)

            if alarm["confirmed_at"] is not None:
                # Idempotent repeat confirmation: report the stored result.
                return self._snapshot(conn, alarm_no, now_real)

            updated = conn.execute(
                """
                UPDATE alarms SET confirmed_at = ?, confirmed_real = ?, confirmed_by = ?
                 WHERE alarm_no = ? AND confirmed_at IS NULL AND escalated_at IS NULL
                """,
                (now, now_real, confirmed_by, alarm_no),
            ).rowcount

            if updated:
                # A still-queued primary (e.g. held by a cabinet silence) must
                # never be paged after the alarm is confirmed.
                cancelled = self._cancel_pending(
                    conn,
                    alarm_no,
                    detail="cancelled before dispatch: alarm confirmed",
                    history="alarm confirmed before dispatch",
                )
                suffix = "".join(
                    f"; queued {role} notification cancelled before dispatch"
                    for role in cancelled
                )
                self._event(
                    conn,
                    alarm_no,
                    "confirmed",
                    f"by {confirmed_by}" + (f": {note}" if note else "") + suffix,
                )
                return self._snapshot(conn, alarm_no, now_real)

            # Escalation already committed. Record the late confirmation but
            # create no new notification. A backup that is still only queued
            # (never handed to the gateway) is cancelled; one that is already
            # in flight is left for reconciliation and shown truthfully.
            conn.execute(
                "UPDATE alarms SET confirmed_at = ?, confirmed_real = ?, confirmed_by = ? "
                " WHERE alarm_no = ? AND confirmed_at IS NULL",
                (now, now_real, confirmed_by, alarm_no),
            )
            cancelled = self._cancel_pending(
                conn,
                alarm_no,
                detail="cancelled before dispatch: alarm confirmed after escalation",
                history="alarm confirmed after escalation, before dispatch",
            )
            backup_row = conn.execute(
                "SELECT state FROM notifications WHERE alarm_no = ? AND role = 'backup'",
                (alarm_no,),
            ).fetchone()
            self._event(
                conn,
                alarm_no,
                "confirmed_after_escalation",
                f"by {confirmed_by} after the escalation deadline"
                + (
                    "; queued backup cancelled before dispatch"
                    if "backup" in cancelled
                    else "; backup already dispatched, cannot be recalled"
                    if backup_row is not None
                    else ""
                ),
            )
            return self._snapshot(conn, alarm_no, now_real)

    # ----------------------------------------------------------------- silence

    def start_silence(self, cabinet: str, reason: str | None, silenced_by: str | None) -> dict[str, Any]:
        """Silence a cabinet. Idempotent: an already-silenced cabinet keeps its
        original record and reports ``created=False``.

        Takes effect at commit: any notification claimed *after* this
        transaction is held; anything claimed before it counts as already
        dispatched and is reconciled against the gateway's real answer.
        """
        cabinet = cabinet.strip()
        if not cabinet:
            raise InvalidStateError("cabinet must not be empty")
        now = utc_now()
        now_real = parse_ts(now)
        with self.db.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM cabinet_silences WHERE cabinet = ?", (cabinet,)
            ).fetchone()
            created = existing is None
            if created:
                conn.execute(
                    "INSERT INTO cabinet_silences (cabinet, silenced_at, silenced_real, silenced_by, reason)"
                    " VALUES (?, ?, ?, ?, ?)",
                    (cabinet, now, now_real, silenced_by, reason),
                )
                self._note_silence_event(
                    conn,
                    cabinet,
                    "silence_started",
                    f"cabinet {cabinet} silenced"
                    + (f" by {silenced_by}" if silenced_by else "")
                    + (f": {reason}" if reason else ""),
                )
            row = conn.execute(
                "SELECT * FROM cabinet_silences WHERE cabinet = ?", (cabinet,)
            ).fetchone()
        view = self._silence_view(dict(row))
        view["created"] = created
        return view

    def stop_silence(self, cabinet: str) -> dict[str, Any]:
        """End a cabinet silence. Idempotent: stopping a cabinet that is not
        silenced succeeds with ``was_silenced=False``. Held notifications become
        dispatchable again on the next worker tick; ones whose alarm was
        confirmed meanwhile were already cancelled and stay cancelled."""
        cabinet = cabinet.strip()
        if not cabinet:
            raise InvalidStateError("cabinet must not be empty")
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM cabinet_silences WHERE cabinet = ?", (cabinet,)
            ).fetchone()
            was_silenced = row is not None
            released = 0
            if was_silenced:
                released = self._held_count(conn, cabinet)
                conn.execute("DELETE FROM cabinet_silences WHERE cabinet = ?", (cabinet,))
                self._note_silence_event(
                    conn,
                    cabinet,
                    "silence_ended",
                    f"cabinet {cabinet} silence ended; {released} held notification(s) released",
                )
        return {
            "cabinet": cabinet,
            "silenced": False,
            "was_silenced": was_silenced,
            "released_notifications": released,
        }

    def get_silence(self, cabinet: str) -> dict[str, Any] | None:
        row = self.db.query_one(
            "SELECT * FROM cabinet_silences WHERE cabinet = ?", (cabinet.strip(),)
        )
        return self._silence_view(dict(row)) if row is not None else None

    def list_silences(self) -> list[dict[str, Any]]:
        rows = self.db.query("SELECT * FROM cabinet_silences ORDER BY silenced_real, cabinet")
        return [self._silence_view(dict(row)) for row in rows]

    def active_silence(self, conn, cabinet: str | None) -> dict[str, Any] | None:
        """Raw silence row for a cabinet, usable inside an open transaction."""
        if not cabinet:
            return None
        sql = "SELECT * FROM cabinet_silences WHERE cabinet = ?"
        if conn is not None:
            row = conn.execute(sql, (cabinet,)).fetchone()
        else:
            row = self.db.query_one(sql, (cabinet,))
        return dict(row) if row is not None else None

    def _silence_view(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "cabinet": row["cabinet"],
            "silenced": True,
            "silenced_at": row["silenced_at"],
            "silenced_by": row["silenced_by"],
            "reason": row["reason"],
            "held_notifications": self._held_count(None, row["cabinet"]),
        }

    def _held_count(self, conn, cabinet: str) -> int:
        """Notifications never dispatched that the silence is currently holding."""
        sql = (
            "SELECT COUNT(*) AS c FROM notifications n "
            " JOIN alarms a ON a.alarm_no = n.alarm_no "
            " WHERE n.state = 'pending' AND a.cabinet = ?"
        )
        if conn is not None:
            row = conn.execute(sql, (cabinet,)).fetchone()
        else:
            row = self.db.query_one(sql, (cabinet,))
        return int(row["c"])

    def _note_silence_event(self, conn, cabinet: str, kind: str, detail: str) -> None:
        """Audit trail on the alarms whose notifications are still in play."""
        rows = conn.execute(
            "SELECT DISTINCT n.alarm_no AS alarm_no FROM notifications n "
            " JOIN alarms a ON a.alarm_no = n.alarm_no "
            " WHERE a.cabinet = ? AND n.state IN ('pending', 'unknown', 'in_flight')",
            (cabinet,),
        ).fetchall()
        for row in rows:
            self._event(conn, row["alarm_no"], kind, detail)

    # ---------------------------------------------------------------- deadline

    def due_escalations(self, now_real: float) -> list[str]:
        rows = self.db.query(
            """
            SELECT alarm_no FROM alarms
             WHERE primary_ack_deadline_real IS NOT NULL
               AND primary_ack_deadline_real <= ?
               AND confirmed_at IS NULL
               AND escalated_at IS NULL
            """,
            (now_real,),
        )
        return [row["alarm_no"] for row in rows]

    def escalate(self, alarm_no: str, now_real: float) -> bool:
        """Create the single backup notification for an overdue alarm.

        Returns True when this call enqueued the backup. The conditional UPDATE
        guarantees exactly one enqueue even with concurrent workers, and loses
        cleanly to a simultaneous confirmation.
        """
        now = utc_now()
        with self.db.transaction() as conn:
            alarm = conn.execute("SELECT * FROM alarms WHERE alarm_no = ?", (alarm_no,)).fetchone()
            if alarm is None:
                return False
            if alarm["confirmed_at"] is not None or alarm["escalated_at"] is not None:
                return False
            if alarm["primary_ack_deadline_real"] is None or alarm["primary_ack_deadline_real"] > now_real:
                return False

            updated = conn.execute(
                "UPDATE alarms SET escalated_at = ?, escalated_real = ? "
                " WHERE alarm_no = ? AND confirmed_at IS NULL AND escalated_at IS NULL",
                (now, now_real, alarm_no),
            ).rowcount
            if not updated:
                return False

            existing = conn.execute(
                "SELECT 1 FROM notifications WHERE alarm_no = ? AND role = 'backup'", (alarm_no,)
            ).fetchone()
            if existing is not None:
                return True

            backup_contact = json.loads(alarm["backup_contact"])
            payload = json.loads(alarm["payload_json"])
            backup = self._notification_row(
                alarm_no=alarm_no,
                role="backup",
                contact=backup_contact,
                content_hash=alarm["content_hash"],
                payload={
                    "content": payload["content"],
                    "metadata": payload.get("metadata", {}),
                    "ack_deadline_seconds": payload["ack_deadline_seconds"],
                    "escalation_reason": "primary_not_confirmed",
                },
                created_by="escalation",
            )
            self._insert_notification(conn, backup)
            self._event(
                conn,
                alarm_no,
                "escalated",
                f"backup notification {backup['notification_no']} queued after deadline",
            )
            return True

    # ------------------------------------------------------------ notification

    def notifications_needing_action(self, now_real: float) -> list[dict[str, Any]]:
        """Pending posts, unknown receipts, and expired in-flight leases.

        Never-posted (``pending``) notifications of a confirmed alarm or a
        silenced cabinet are excluded — they are held, not due. Notifications
        with an ambiguous receipt stay eligible regardless of silence: an
        initiated notification must keep being reconciled against the gateway's
        real answer. The claim transaction re-checks the same conditions, so
        this filter is only an optimisation.
        """
        rows = self.db.query(
            """
            SELECT n.* FROM notifications n
            JOIN alarms a ON a.alarm_no = n.alarm_no
             WHERE (
                    (n.state = 'pending' AND (n.next_attempt_at IS NULL OR n.next_attempt_at <= ?)
                     AND a.confirmed_at IS NULL
                     AND NOT EXISTS (
                         SELECT 1 FROM cabinet_silences s WHERE s.cabinet = a.cabinet
                     ))
                OR (n.state = 'unknown' AND (n.next_attempt_at IS NULL OR n.next_attempt_at <= ?))
                OR (n.state = 'in_flight' AND (n.lease_until IS NULL OR n.lease_until <= ?))
             )
             ORDER BY n.created_real, n.id
            """,
            (now_real, now_real, now_real),
        )
        return [dict(row) for row in rows]

    def claim_notification(
        self,
        notification_id: int,
        now_real: float,
        lease_seconds: float,
        *,
        counts_as_post_attempt: bool = False,
    ) -> bool:
        """Move a notification to in_flight. Must happen outside any HTTP wait.

        ``attempts`` counts gateway POSTs only; claiming a row to *look up* an
        unresolved receipt is not a delivery attempt.

        A never-posted (``pending``) row is not claimable while its alarm is
        confirmed or its cabinet is silenced. The check runs inside this write
        transaction, so a concurrent confirmation or silence start is
        serialised against the claim: whichever commits first wins. Rows with
        an ambiguous receipt (``unknown``/expired ``in_flight``) stay claimable
        under silence — they are looked up, not re-posted.
        """
        with self.db.transaction() as conn:
            row = conn.execute(
                """
                SELECT n.state, n.lease_until, a.confirmed_at,
                       EXISTS(
                           SELECT 1 FROM cabinet_silences s WHERE s.cabinet = a.cabinet
                       ) AS silenced
                  FROM notifications n
                  JOIN alarms a ON a.alarm_no = n.alarm_no
                 WHERE n.id = ?
                """,
                (notification_id,),
            ).fetchone()
            if row is None:
                return False
            if row["state"] == "pending" and (row["confirmed_at"] is not None or row["silenced"]):
                return False
            claimable = row["state"] in {"pending", "unknown"} or (
                row["state"] == "in_flight" and (row["lease_until"] is None or row["lease_until"] <= now_real)
            )
            if not claimable:
                return False
            conn.execute(
                "UPDATE notifications SET state = 'in_flight', lease_until = ?, last_attempt_at = ?, "
                " attempts = attempts + ? WHERE id = ?",
                (
                    now_real + lease_seconds,
                    utc_now(),
                    1 if counts_as_post_attempt else 0,
                    notification_id,
                ),
            )
            return True

    def repost_allowed(self, notification_id: int) -> tuple[bool, str | None]:
        """Whether an unknown-receipt notification may be re-posted right now.

        Re-posting is a *new* delivery to the gateway, so it is suppressed
        while the alarm is confirmed or its cabinet is silenced — exactly the
        conditions under which a never-posted notification would be held.
        Lookup-based reconciliation is never affected by this gate, and when
        the suppression lifts, the re-post still uses the original notification
        number.
        """
        row = self.db.query_one(
            """
            SELECT n.state, a.confirmed_at,
                   EXISTS(SELECT 1 FROM cabinet_silences s WHERE s.cabinet = a.cabinet) AS silenced
              FROM notifications n
              JOIN alarms a ON a.alarm_no = n.alarm_no
             WHERE n.id = ?
            """,
            (notification_id,),
        )
        if row is None or row["state"] not in {"unknown", "in_flight"}:
            # Already resolved (accepted/failed/cancelled) by another path.
            # in_flight is the dispatcher's own claim around the lookup+repost.
            return False, "notification state changed"
        if row["confirmed_at"] is not None:
            return False, "alarm confirmed"
        if row["silenced"]:
            return False, "cabinet silenced"
        return True, None

    def note_post_attempt(self, notification_id: int) -> None:
        """Count a real gateway POST. Committed before the call, so even a crash
        in the middle of the request leaves an audit trail."""
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE notifications SET attempts = attempts + 1, last_attempt_at = ? WHERE id = ?",
                (utc_now(), notification_id),
            )

    def mark_notification(
        self,
        notification_id: int,
        state: str,
        gateway_status: str | None,
        detail: str | None,
        *,
        accepted_real: float | None = None,
        retry_at: float | None = None,
        lookup_miss: bool = False,
    ) -> dict[str, Any] | None:
        """Persist an outcome. A confirmed primary acceptance starts the clock
        exactly once, atomically with the state change."""
        now = utc_now()
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM notifications WHERE id = ?", (notification_id,)
            ).fetchone()
            if row is None:
                return None

            history = self._append_history(
                row["history_json"],
                state,
                f"gateway_status={gateway_status}" + (f"; {detail}" if detail else ""),
            )
            accepted_at = row["accepted_at"]
            accepted_real_value = row["accepted_real"]
            if state == "accepted" and accepted_at is None:
                accepted_real_value = accepted_real if accepted_real is not None else parse_ts(now)
                accepted_at = iso_from_real(accepted_real_value)

            next_attempt_at = None
            if state in {"pending", "unknown", "in_flight"}:
                next_attempt_at = retry_at if retry_at is not None else parse_ts(now)

            if lookup_miss:
                lookup_misses = row["lookup_misses"] + 1
            elif state in {"accepted", "pending", "failed"}:
                # A fresh post or a resolution resets the miss counter.
                lookup_misses = 0
            else:
                lookup_misses = row["lookup_misses"]

            conn.execute(
                """
                UPDATE notifications
                   SET state = ?, gateway_status = ?, detail = ?, history_json = ?,
                       next_attempt_at = ?, accepted_at = ?, accepted_real = ?,
                       lease_until = NULL, lookup_misses = ?
                 WHERE id = ?
                """,
                (
                    state,
                    gateway_status,
                    detail,
                    history,
                    next_attempt_at,
                    accepted_at,
                    accepted_real_value,
                    lookup_misses,
                    notification_id,
                ),
            )

            if state == "accepted":
                if row["role"] == "primary":
                    deadline_seconds = self._deadline_seconds(conn, row["alarm_no"])
                    accepted_real_value = accepted_real_value or parse_ts(now)
                    conn.execute(
                        """
                        UPDATE alarms
                           SET primary_accepted_at = COALESCE(primary_accepted_at, ?),
                               primary_accepted_real = COALESCE(primary_accepted_real, ?),
                               primary_ack_deadline_at = COALESCE(primary_ack_deadline_at, ?),
                               primary_ack_deadline_real = COALESCE(primary_ack_deadline_real, ?)
                         WHERE alarm_no = ?
                        """,
                        (
                            iso_from_real(accepted_real_value),
                            accepted_real_value,
                            iso_from_real(accepted_real_value + deadline_seconds),
                            accepted_real_value + deadline_seconds,
                            row["alarm_no"],
                        ),
                    )
                    self._event(
                        conn,
                        row["alarm_no"],
                        "primary_accepted",
                        f"{row['notification_no']} accepted; {deadline_seconds}s confirmation clock started",
                    )
                else:
                    self._event(
                        conn,
                        row["alarm_no"],
                        "backup_accepted",
                        f"{row['notification_no']} accepted",
                    )
            elif state == "failed":
                self._event(
                    conn,
                    row["alarm_no"],
                    f"{row['role']}_rejected",
                    f"{row['notification_no']}: {detail}",
                )
            elif state == "unknown":
                self._event(
                    conn,
                    row["alarm_no"],
                    f"{row['role']}_receipt_unknown",
                    f"{row['notification_no']}: {detail} (no escalation timer change)",
                )

            return dict(
                conn.execute("SELECT * FROM notifications WHERE id = ?", (notification_id,)).fetchone()
            )

    # --------------------------------------------------------------- snapshot

    def snapshot(self, alarm_no: str, now_real: float | None = None) -> dict[str, Any]:
        return self._snapshot(None, alarm_no, now_real or parse_ts(utc_now()))

    def _snapshot(self, conn, alarm_no: str, now_real: float) -> dict[str, Any]:
        def q(sql: str, params: tuple) -> list[Any]:
            if conn is not None:
                return list(conn.execute(sql, params).fetchall())
            return self.db.query(sql, params)

        alarm = (q("SELECT * FROM alarms WHERE alarm_no = ?", (alarm_no,)) or [None])[0]
        if alarm is None:
            raise NotFoundError(alarm_no)
        alarm = dict(alarm)

        notifications = q(
            "SELECT * FROM notifications WHERE alarm_no = ? ORDER BY id", (alarm_no,)
        )
        events = q(
            "SELECT at, kind, detail FROM events WHERE alarm_no = ? ORDER BY id", (alarm_no,)
        )
        silence = self.active_silence(conn, alarm.get("cabinet"))
        return build_alarm_view(
            alarm,
            [dict(n) for n in notifications],
            [dict(e) for e in events],
            now_real=now_real,
            silence=silence,
        )
