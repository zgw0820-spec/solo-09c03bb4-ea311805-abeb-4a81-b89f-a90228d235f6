"""Cabinet silence: pause a cabinet's notifications during maintenance.

Semantics under test:

* After a silence takes effect, notifications never handed to the gateway
  (``pending``) are held; other cabinets are unaffected.
* Notifications already on the wire keep being reconciled against the
  gateway's real answer — silence never fakes a cancellation.
* The confirmation clock runs as usual during silence; an expiring deadline
  creates the backup row but its delivery is deferred.
* Views expose the silence and the held-for-delivery state.
* Ending the silence: confirmed alarms are not paged, unconfirmed ones resume
  delivery on the original notification number, and the overdue backup goes
  out at most once.
* Silences are persisted, so an API restart keeps honouring them.
"""

from __future__ import annotations

import asyncio

from tests.conftest import ApiHarness
from tests.helpers import _get, _status
from tests.test_recovery import RestartableApi
from tests.fake_gateway import FakeGateway


async def _pred(api, alarm_no: str, predicate) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    return response.status_code == 200 and predicate(response.json())


def _gateway_records(api, alarm_no: str) -> list[dict]:
    return [r for r in api.fake.records.values() if r["alarm_no"] == alarm_no]


async def test_silence_holds_pending_primary_and_other_cabinets_unaffected(api: ApiHarness):
    start = await api.client.put(
        "/silences/A07", json={"reason": "供电柜检修", "silenced_by": "王工"}
    )
    assert start.status_code == 200, start.text
    body = start.json()
    assert body["silenced"] is True
    assert body["created"] is True
    assert body["reason"] == "供电柜检修"
    assert body["silenced_by"] == "王工"

    # Idempotent re-start keeps the original record.
    again = await api.client.put("/silences/A07", json={"reason": "重复设置"})
    assert again.status_code == 200
    assert again.json()["created"] is False
    assert again.json()["reason"] == "供电柜检修"

    await api.client.post("/alarms", json=api.alarm_payload("SIL-1", deadline=5))
    await api.client.post(
        "/alarms", json=api.alarm_payload("SIL-2", deadline=5, metadata={"cabinet": "B02"})
    )

    # The other cabinet notifies as usual.
    assert await api.wait_until(lambda: _status(api, "SIL-2", "awaiting_confirmation"))

    # The silenced cabinet's primary stays queued and never reaches the gateway.
    await asyncio.sleep(0.4)
    view = await _get(api, "SIL-1")
    assert view["silenced"] is True
    assert view["cabinet"] == "A07"
    assert view["silence"]["reason"] == "供电柜检修"
    assert view["status"] == "primary_queued"
    primary = view["notifications"][0]
    assert primary["state"] == "pending"
    assert primary["display_status"] == "silenced_hold"
    assert any("静默" in w for w in view["warnings"])
    assert _gateway_records(api, "SIL-1") == []

    # Silence is visible via list and single-cabinet queries, with held counts.
    listed = (await api.client.get("/silences")).json()
    assert listed["total"] == 1
    assert listed["items"][0]["cabinet"] == "A07"
    assert listed["items"][0]["held_notifications"] == 1
    one = (await api.client.get("/silences/A07")).json()
    assert one["held_notifications"] == 1

    # Ending the silence releases the held primary; it is delivered once.
    stop = await api.client.delete("/silences/A07")
    assert stop.status_code == 200
    assert stop.json()["was_silenced"] is True
    assert stop.json()["released_notifications"] == 1
    assert await api.wait_until(lambda: _status(api, "SIL-1", "awaiting_confirmation"))
    primaries = [r for r in _gateway_records(api, "SIL-1") if r["role"] == "primary"]
    assert len(primaries) == 1

    # Stopping again is a no-op; the cabinet no longer shows as silenced.
    assert (await api.client.delete("/silences/A07")).json()["was_silenced"] is False
    assert (await api.client.get("/silences/A07")).status_code == 404
    view = await _get(api, "SIL-1")
    assert view["silenced"] is False
    assert view["silence"] is None


async def test_deadline_fires_during_silence_and_backup_is_held_until_release(api: ApiHarness):
    """Primary accepted before the silence: the confirmation clock keeps
    running, the deadline fires during the silence, the backup row is created
    but not sent, and after release it goes out exactly once."""
    await api.client.post("/alarms", json=api.alarm_payload("SIL-ESC", deadline=2))
    assert await api.wait_until(lambda: _status(api, "SIL-ESC", "awaiting_confirmation"))

    await api.client.put("/silences/A07", json={"reason": "检修"})

    # The deadline expires during the silence: escalation happens on schedule,
    # but the backup notification is held, not delivered.
    assert await api.wait_until(
        lambda: _pred(api, "SIL-ESC", lambda v: v["escalated"] is True), timeout=6
    ), await _get(api, "SIL-ESC")
    view = await _get(api, "SIL-ESC")
    assert view["silenced"] is True
    backup = next(n for n in view["notifications"] if n["role"] == "backup")
    assert backup["state"] == "pending"
    assert backup["display_status"] == "silenced_hold"
    assert any("暂缓" in w for w in view["warnings"])
    await asyncio.sleep(0.5)
    assert api.fake.role_notifications("backup") == []

    # Release: the overdue backup is delivered exactly once.
    stop = await api.client.delete("/silences/A07")
    assert stop.json()["released_notifications"] == 1
    assert await api.wait_until(
        lambda: _pred(
            api,
            "SIL-ESC",
            lambda v: any(
                n["role"] == "backup" and n["state"] == "accepted" for n in v["notifications"]
            ),
        ),
        timeout=6,
    ), await _get(api, "SIL-ESC")
    await asyncio.sleep(0.5)
    backups = api.fake.role_notifications("backup")
    assert len(backups) == 1
    view = await _get(api, "SIL-ESC")
    assert [n["notification_no"] for n in view["notifications"] if n["role"] == "backup"] == [
        backups[0]["notification_no"]
    ]


async def test_confirm_during_silence_cancels_held_primary_and_nothing_is_sent(api: ApiHarness):
    """An alarm confirmed while its primary is still held must not be paged
    after the silence ends."""
    await api.client.put("/silences/A07", json={"reason": "检修"})
    await api.client.post("/alarms", json=api.alarm_payload("SIL-CF", deadline=2))

    confirm = await api.client.post("/alarms/SIL-CF/confirm", json={"confirmed_by": "张工"})
    assert confirm.status_code == 200
    assert confirm.json()["status"] == "confirmed"
    primary = confirm.json()["notifications"][0]
    assert primary["state"] == "cancelled"
    assert primary["display_status"] == "cancelled_before_dispatch"

    await api.client.delete("/silences/A07")
    await asyncio.sleep(0.8)
    assert _gateway_records(api, "SIL-CF") == [], "confirmed alarm must not be paged after release"
    view = await _get(api, "SIL-CF")
    assert view["status"] == "confirmed"
    assert view["escalated"] is False


async def test_confirm_after_escalation_during_silence_cancels_held_backup(api: ApiHarness):
    """Deadline fired during silence, backup held; a confirmation before the
    release cancels the queued backup and it is never sent."""
    await api.client.post("/alarms", json=api.alarm_payload("SIL-CFE", deadline=2))
    assert await api.wait_until(lambda: _status(api, "SIL-CFE", "awaiting_confirmation"))
    await api.client.put("/silences/A07", json={"reason": "检修"})
    assert await api.wait_until(
        lambda: _pred(api, "SIL-CFE", lambda v: v["escalated"] is True), timeout=6
    )

    confirm = await api.client.post("/alarms/SIL-CFE/confirm", json={"confirmed_by": "张工"})
    assert confirm.status_code == 200
    backup = next(n for n in confirm.json()["notifications"] if n["role"] == "backup")
    assert backup["state"] == "cancelled"

    await api.client.delete("/silences/A07")
    await asyncio.sleep(0.8)
    assert api.fake.role_notifications("backup") == []
    view = await _get(api, "SIL-CFE")
    assert view["status"] == "confirmed"


async def test_inflight_primary_is_reconciled_during_silence_not_recalled(api: ApiHarness):
    """A primary already on the wire when the silence starts is not recalled:
    its true gateway result is adopted and the clock runs as usual."""
    api.fake.slow_seconds = 0.8
    await api.client.post("/alarms", json=api.alarm_payload("SIL-INF", deadline=2))
    assert await api.wait_until(
        lambda: _pred(
            api, "SIL-INF", lambda v: v["notifications"][0]["state"] == "in_flight"
        ),
        timeout=5,
    ), await _get(api, "SIL-INF")

    await api.client.put("/silences/A07", json={"reason": "检修"})
    api.fake.slow_seconds = 0.0

    # The in-flight POST completes during the silence and is adopted truthfully.
    assert await api.wait_until(
        lambda: _pred(api, "SIL-INF", lambda v: v["primary_accepted_at"] is not None),
        timeout=6,
    ), await _get(api, "SIL-INF")
    view = await _get(api, "SIL-INF")
    assert view["silenced"] is True
    assert view["status"] == "awaiting_confirmation"
    assert len([r for r in _gateway_records(api, "SIL-INF") if r["role"] == "primary"]) == 1

    # The clock kept running: the deadline fires during the silence and the
    # backup is created but held.
    assert await api.wait_until(
        lambda: _pred(api, "SIL-INF", lambda v: v["escalated"] is True), timeout=6
    )
    await asyncio.sleep(0.4)
    assert api.fake.role_notifications("backup") == []


async def test_unknown_receipt_repost_deferred_by_silence_and_resumed_on_same_number(
    api: ApiHarness,
):
    """A lost POST (gateway has no record) is not re-posted while the cabinet
    is silenced; after release the retry uses the original notification number."""
    db = api.app.state.db
    await api.client.put("/silences/A07", json={"reason": "检修"})
    submit = await api.client.post("/alarms", json=api.alarm_payload("SIL-UNK", deadline=5))
    notification_no = submit.json()["notifications"][0]["notification_no"]

    # Simulate a lost POST: the row is unknown but the gateway never saw it.
    db.execute(
        "UPDATE notifications SET state = 'unknown', next_attempt_at = 0 "
        "WHERE notification_no = ?",
        (notification_no,),
    )
    await asyncio.sleep(1.0)  # several lookup rounds; gateway keeps answering 404
    view = await _get(api, "SIL-UNK")
    primary = view["notifications"][0]
    assert primary["state"] == "unknown"
    assert "deferred" in (primary["detail"] or "")
    assert api.fake.post_calls == 0, "a silenced cabinet must not be re-posted"

    # Release: the retry goes out on the original notification number.
    await api.client.delete("/silences/A07")
    assert await api.wait_until(
        lambda: _pred(api, "SIL-UNK", lambda v: v["primary_accepted_at"] is not None),
        timeout=8,
    ), await _get(api, "SIL-UNK")
    view = await _get(api, "SIL-UNK")
    assert view["notifications"][0]["notification_no"] == notification_no
    primaries = api.fake.role_notifications("primary")
    assert len(primaries) == 1
    assert primaries[0]["notification_no"] == notification_no


async def test_silence_survives_api_restart(tmp_path):
    """Silences live in the database: a restarted API keeps holding the
    cabinet's notifications until the silence is ended."""
    fake = FakeGateway()
    db_path = str(tmp_path / "alarms.db")
    first = await RestartableApi(db_path, fake, "http://gw.test").start()
    await first.client.put("/silences/B12", json={"reason": "检修"})
    payload = {
        "alarm_no": "R-SIL",
        "content": "供电柜 B12 温度 82°C",
        "metadata": {"cabinet": "B12"},
        "primary_contact": {"name": "主值班员", "channel": "app"},
        "backup_contact": {"name": "备班", "channel": "sms"},
        "ack_deadline_seconds": 5,
    }
    await first.client.post("/alarms", json=payload)
    await asyncio.sleep(0.3)
    assert fake.records == {}
    await first.stop()

    second = await RestartableApi(db_path, fake, "http://gw.test").start()
    try:
        await asyncio.sleep(0.6)  # several worker ticks after the restart
        assert fake.records == {}, "held notification must survive the restart unsent"
        view = (await second.client.get("/alarms/R-SIL")).json()
        assert view["silenced"] is True
        assert view["notifications"][0]["state"] == "pending"
        assert view["notifications"][0]["display_status"] == "silenced_hold"

        await second.client.delete("/silences/B12")
        assert await second.wait_until(
            lambda: _accepted(second, "R-SIL"), timeout=8
        ), (await second.client.get("/alarms/R-SIL")).text
        assert len(fake.role_notifications("primary")) == 1
    finally:
        await second.stop()


async def test_silence_start_stop_and_lookup_api_surface(api: ApiHarness):
    # Unknown cabinet lookup -> 404; empty list.
    assert (await api.client.get("/silences/NOPE")).status_code == 404
    assert (await api.client.get("/silences")).json() == {"items": [], "total": 0}

    # Body is optional.
    assert (await api.client.put("/silences/C01")).status_code == 200
    # Whitespace-only cabinet is rejected.
    assert (await api.client.put("/silences/%20%20")).status_code == 422
    # Extra fields are rejected.
    bad = await api.client.put("/silences/C02", json={"bogus": 1})
    assert bad.status_code == 422

    # OpenAPI exposes the silence endpoints.
    schema = (await api.client.get("/openapi.json")).json()
    assert "/silences" in schema["paths"]
    assert "/silences/{cabinet}" in schema["paths"]


async def test_list_alarms_filter_by_cabinet(api: ApiHarness):
    await api.client.post(
        "/alarms", json=api.alarm_payload("CF-1", deadline=5, metadata={"cabinet": "C01"})
    )
    await api.client.post(
        "/alarms", json=api.alarm_payload("CF-2", deadline=5, metadata={"cabinet": "C02"})
    )
    filtered = (await api.client.get("/alarms", params={"cabinet": "C01"})).json()
    assert filtered["total"] == 1
    assert filtered["items"][0]["alarm_no"] == "CF-1"
    assert filtered["items"][0]["cabinet"] == "C01"


async def test_existing_database_is_migrated_with_cabinet_backfilled(tmp_path):
    """A database created before cabinet silences existed gains the column
    (backfilled from metadata.cabinet) and the silence table, and silences
    apply to pre-existing alarms."""
    import json
    import sqlite3

    path = str(tmp_path / "old.db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE alarms (
            alarm_no TEXT PRIMARY KEY, content_hash TEXT NOT NULL, payload_json TEXT NOT NULL,
            primary_contact TEXT NOT NULL, backup_contact TEXT NOT NULL,
            ack_deadline_seconds INTEGER NOT NULL, created_at TEXT NOT NULL,
            created_real REAL NOT NULL, primary_accepted_at TEXT, primary_accepted_real REAL,
            primary_ack_deadline_at TEXT, primary_ack_deadline_real REAL,
            confirmed_at TEXT, confirmed_real REAL, confirmed_by TEXT,
            escalated_at TEXT, escalated_real REAL
        );
        CREATE TABLE notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT, alarm_no TEXT NOT NULL,
            notification_no TEXT NOT NULL UNIQUE, role TEXT NOT NULL, contact_json TEXT NOT NULL,
            content_hash TEXT NOT NULL, payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL, created_real REAL NOT NULL, state TEXT NOT NULL,
            gateway_status TEXT, detail TEXT, attempts INTEGER NOT NULL DEFAULT 0,
            created_by TEXT NOT NULL, history_json TEXT NOT NULL DEFAULT '[]',
            accepted_at TEXT, accepted_real REAL, last_attempt_at TEXT,
            lease_until REAL, next_attempt_at REAL
        );
        CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, alarm_no TEXT NOT NULL,
                             at TEXT NOT NULL, kind TEXT NOT NULL, detail TEXT);
        """
    )
    conn.execute(
        "INSERT INTO alarms (alarm_no, content_hash, payload_json, primary_contact,"
        " backup_contact, ack_deadline_seconds, created_at, created_real)"
        " VALUES (?,?,?,?,?,?,?,?)",
        (
            "OLD-1",
            "h",
            json.dumps({"content": "x", "metadata": {"cabinet": "C99"}, "ack_deadline_seconds": 5}),
            "{}",
            "{}",
            5,
            "2026-11-17T00:00:00.000Z",
            1.0,
        ),
    )
    conn.commit()
    conn.close()

    from app.db import Database
    from app.service import AlarmService

    db = Database(path)  # migration runs here
    try:
        assert db.query_one("SELECT cabinet FROM alarms WHERE alarm_no = 'OLD-1'")["cabinet"] == "C99"
        service = AlarmService(db)
        service.start_silence("C99", "检修", None)
        assert service.snapshot("OLD-1")["silenced"] is True
    finally:
        db.close()
    Database(path).close()  # re-opening is idempotent


async def _accepted(api, alarm_no: str) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    if response.status_code != 200:
        return False
    return response.json()["primary_accepted_at"] is not None
