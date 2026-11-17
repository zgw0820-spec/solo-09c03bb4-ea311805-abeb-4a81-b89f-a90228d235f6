"""Translate persisted rows into an operator-facing alarm view.

This module is deliberately the only place that decides how a raw notification
state is phrased. Two rules matter:

* ``receipt_unconfirmed`` is never presented as delivered, and it never implies
  the escalation clock is running.
* an in-flight notification that could not be recalled after a late
  confirmation is reported as such, with the gateway's actual result once known.

Cabinet silences add a third rule: a notification that has never been handed
to the gateway while its cabinet is silenced is shown as held
(``silenced_hold``) — it is queued work, not a cancelled one, and it will be
delivered when the silence ends unless the alarm is confirmed first.
"""

from __future__ import annotations

import json
from typing import Any

NOTIFICATION_DISPLAY = {
    "pending": ("queued", "待发送"),
    "in_flight": ("sending", "发送中"),
    "unknown": ("receipt_unconfirmed", "回执未确认"),
    "accepted": ("accepted", "网关已受理"),
    "failed": ("rejected", "网关拒收"),
    "cancelled": ("cancelled_before_dispatch", "未投递即取消"),
}

ALARM_STATUS_TEXT = {
    "primary_queued": "主通知待发送",
    "primary_sending": "主通知发送中",
    "primary_unconfirmed": "主通知回执未确认，计时未开始",
    "primary_rejected": "主通知被网关拒收",
    "awaiting_confirmation": "主通知已受理，等待确认",
    "escalated": "逾期未确认，已通知备班",
    "confirmed": "已确认",
    "confirmed_after_escalation": "逾期后补确认（备班通知已发出或在途）",
}


def _load(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except ValueError:
        return default


def _notification_view(row: dict[str, Any], confirmed_at: str | None, silenced: bool) -> dict[str, Any]:
    state = row["state"]
    display, display_text = NOTIFICATION_DISPLAY.get(state, (state, state))
    accepted_at = row.get("accepted_at")

    delivered_after_confirmation = bool(
        row["role"] == "backup"
        and accepted_at
        and confirmed_at
        and accepted_at > confirmed_at
    )
    if state == "pending" and silenced:
        # Never handed to the gateway and currently held by a cabinet silence.
        display, display_text = "silenced_hold", "静默中，暂缓投递"
    elif row["role"] == "primary" and state == "cancelled":
        display_text = "未投递即取消（告警已确认）"
    elif row["role"] == "backup" and state == "cancelled":
        display_text = "未投递即取消（确认早于发送）"
    elif delivered_after_confirmation:
        display_text = "确认后才被网关受理（在途，无法撤回）"

    return {
        "notification_no": row["notification_no"],
        "role": row["role"],
        "contact": _load(row["contact_json"], {}),
        "state": state,
        "display_status": display,
        "display_text": display_text,
        "gateway_status": row.get("gateway_status"),
        "detail": row.get("detail"),
        "attempts": row.get("attempts", 0),
        "created_at": row["created_at"],
        "accepted_at": accepted_at,
        "last_attempt_at": row.get("last_attempt_at"),
        "history": _load(row.get("history_json"), []),
        "delivered_after_confirmation": delivered_after_confirmation,
    }


def build_alarm_view(
    alarm: dict[str, Any],
    notifications: list[dict[str, Any]],
    events: list[dict[str, Any]],
    *,
    now_real: float,
    silence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    by_role = {n["role"]: n for n in notifications}
    primary = by_role.get("primary")
    backup = by_role.get("backup")

    confirmed = alarm["confirmed_at"] is not None
    escalated = alarm["escalated_at"] is not None
    accepted_real = alarm.get("primary_accepted_real")
    cabinet = alarm.get("cabinet")
    silenced = silence is not None

    warnings: list[str] = []
    if confirmed and escalated:
        status = "confirmed_after_escalation"
    elif confirmed:
        status = "confirmed"
    elif escalated:
        status = "escalated"
    elif accepted_real is not None:
        status = "awaiting_confirmation"
    elif primary is None:
        status = "primary_queued"
    elif primary["state"] in {"pending", "in_flight"}:
        status = "primary_sending" if primary["state"] == "in_flight" else "primary_queued"
    elif primary["state"] == "failed":
        status = "primary_rejected"
    else:
        status = "primary_unconfirmed"

    if confirmed and backup is not None and backup["state"] == "cancelled":
        # The deadline fired but the confirmation cancelled the queued backup
        # before it was handed to the gateway: no page actually went out, so the
        # alarm is simply confirmed. The warning below records the cancelled row.
        status = "confirmed"

    if primary is not None and primary["state"] in {"pending", "in_flight", "unknown"}:
        warnings.append("主通知尚未获得网关受理回执，确认计时尚未开始；回执不明不会被当作已送达。")
    if primary is not None and primary["state"] == "unknown":
        warnings.append("主通知回执不明，正在向网关查询受理结果；查明前不会触发升级。")
    if primary is not None and primary["state"] == "failed":
        warnings.append("网关明确拒收主通知，系统不会自动重发，请人工核实主备联系人。")
    if primary is not None and primary["state"] == "cancelled":
        warnings.append("主通知未投递即取消（告警已确认）。")
    if backup is not None and backup["state"] in {"in_flight", "unknown"}:
        warnings.append("备班通知已提交且在途，受理结果正在向网关查明，请以查询结果为准。")
    if (
        backup is not None
        and confirmed
        and backup["state"] == "accepted"
        and backup.get("accepted_at")
        and alarm.get("confirmed_at")
        and backup["accepted_at"] > alarm["confirmed_at"]
    ):
        warnings.append("告警确认时备班通知已在途，网关仍受理了该通知，无法撤回。")
    if backup is not None and backup["state"] == "cancelled":
        warnings.append("告警已确认，排队中的备班通知未投递即取消。")
    if backup is not None and backup["state"] == "failed":
        warnings.append("网关拒收备班通知，升级投递失败，需人工通知备班。")
    if silenced:
        held = sum(1 for n in notifications if n["state"] == "pending")
        label = f"柜 {cabinet} 静默中"
        if silence.get("reason"):
            label += f"（{silence['reason']}）"
        if held:
            warnings.append(
                f"{label}：{held} 条通知暂缓投递；静默结束后，未确认告警的通知将继续投递，"
                "已确认告警不再补发。"
            )
        else:
            warnings.append(f"{label}。")
        if escalated and backup is not None and backup["state"] == "pending":
            warnings.append("确认期限已到期，备班通知已生成但因静默暂缓发送。")
        if any(n["state"] in {"in_flight", "unknown"} for n in notifications):
            warnings.append("已发起的通知不受静默影响，仍按网关真实受理结果核查，不会冒称取消。")

    seconds_until: float | None = None
    if (
        alarm.get("primary_ack_deadline_real") is not None
        and not confirmed
        and not escalated
    ):
        seconds_until = round(max(0.0, alarm["primary_ack_deadline_real"] - now_real), 3)

    return {
        "alarm_no": alarm["alarm_no"],
        "status": status,
        "status_text": ALARM_STATUS_TEXT.get(status, status),
        "content": _load(alarm["payload_json"], {}).get("content", ""),
        "metadata": _load(alarm["payload_json"], {}).get("metadata", {}),
        "cabinet": cabinet,
        "silenced": silenced,
        "silence": (
            {
                "cabinet": silence["cabinet"],
                "silenced_at": silence["silenced_at"],
                "silenced_by": silence["silenced_by"],
                "reason": silence["reason"],
            }
            if silence
            else None
        ),
        "primary_contact": _load(alarm["primary_contact"], {}),
        "backup_contact": _load(alarm["backup_contact"], {}),
        "ack_deadline_seconds": alarm["ack_deadline_seconds"],
        "created_at": alarm["created_at"],
        "primary_accepted_at": alarm.get("primary_accepted_at"),
        "confirmation_due_at": alarm.get("primary_ack_deadline_at"),
        "seconds_until_escalation": seconds_until,
        "confirmed": confirmed,
        "confirmed_at": alarm.get("confirmed_at"),
        "confirmed_by": alarm.get("confirmed_by"),
        "escalated": escalated,
        "escalated_at": alarm.get("escalated_at"),
        "notifications": [
            _notification_view(n, alarm.get("confirmed_at"), silenced) for n in notifications
        ],
        "events": events,
        "warnings": warnings,
        "deduplicated": False,
    }
