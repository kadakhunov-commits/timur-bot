#!/usr/bin/env python3
from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utcnow().isoformat()


def parse_iso(raw: str) -> datetime:
    return datetime.fromisoformat(raw)


class BillingError(Exception):
    pass


@dataclass
class QuoteResult:
    chat_id: int
    mode: str
    provider: str
    active_users: List[int]
    payer_ids: List[int]
    total_rub: int
    price_per_active_rub: int
    min_price_rub: int
    max_price_rub: int
    activation_ratio: float


class BillingEngine:
    """Mock-ready billing core with scaling-oriented schema.

    Supports:
    - owner pay or split pay billing modes
    - free promo tier with ad watermark policy
    - active-user pricing
    - invoice ledger and entitlement state
    - abuse/risk controls and cooldowns
    - referral program and affiliate commissions
    """

    VERSION = 1

    def __init__(self, storage_path: Path, logger: Any = None):
        self.storage_path = Path(storage_path)
        self.logger = logger

    # ----------------------------
    # Persistence
    # ----------------------------
    def _default_state(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "config": {
                "pricing": {
                    "per_active_rub": 45,
                    "min_group_rub": 199,
                    "max_group_rub": 3490,
                    "active_window_days": 30,
                    "active_threshold_messages": 3,
                },
                "split": {
                    "activation_ratio": 1.0,
                    "change_cooldown_hours": 24,
                    "max_invoice_batches_per_chat_per_day": 3,
                },
                "free_promo": {
                    "enabled": True,
                    "daily_quota": 30,
                    "watermark_every_n": 4,
                    "watermark_text": "promo: upgrade to remove limits",
                },
                "providers": {
                    "stars": {"enabled": True},
                    "yookassa": {"enabled": True},
                },
                "affiliate": {
                    "default_commission_pct": 10,
                    "default_duration_months": 3,
                    "max_commission_pct": 30,
                },
            },
            "chats": {},
            "subscriptions": {},
            "invoices": {},
            "ledger": [],
            "abuse": {
                "invoice_batches_per_day": {},
                "last_plan_change_ts": {},
                "flags": [],
            },
            "referrals": {
                "programs": {},
                "codes": {},
                "user_attribution": {},
                "affiliate_balances": {},
                "events": [],
            },
        }

    def _load(self) -> Dict[str, Any]:
        if not self.storage_path.exists():
            return self._default_state()
        try:
            with open(self.storage_path, "r", encoding="utf-8") as f:
                state = json.load(f)
            self._ensure_schema(state)
            return state
        except Exception:
            # fallback safe path
            return self._default_state()

    def _save(self, state: Dict[str, Any]) -> None:
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.storage_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

    def _ensure_schema(self, state: Dict[str, Any]) -> None:
        defaults = self._default_state()
        state.setdefault("version", defaults["version"])
        state.setdefault("config", defaults["config"])
        state.setdefault("chats", {})
        state.setdefault("subscriptions", {})
        state.setdefault("invoices", {})
        state.setdefault("ledger", [])
        state.setdefault("abuse", defaults["abuse"])
        state.setdefault("referrals", defaults["referrals"])

        cfg = state["config"]
        cfg.setdefault("pricing", defaults["config"]["pricing"])
        cfg.setdefault("split", defaults["config"]["split"])
        cfg.setdefault("free_promo", defaults["config"]["free_promo"])
        cfg.setdefault("providers", defaults["config"]["providers"])
        cfg.setdefault("affiliate", defaults["config"]["affiliate"])

        abuse = state["abuse"]
        abuse.setdefault("invoice_batches_per_day", {})
        abuse.setdefault("last_plan_change_ts", {})
        abuse.setdefault("flags", [])

        refs = state["referrals"]
        refs.setdefault("programs", {})
        refs.setdefault("codes", {})
        refs.setdefault("user_attribution", {})
        refs.setdefault("affiliate_balances", {})
        refs.setdefault("events", [])

        for chat_id, chat in state["chats"].items():
            chat.setdefault("users", {})
            chat.setdefault("entitlement", None)
            chat.setdefault("metrics", {})
            self._ensure_chat_metrics(chat)
            # prune old activity windows on load
            self._prune_chat_activity(state, int(chat_id), save=False)

    def _ensure_chat_metrics(self, chat: Dict[str, Any]) -> None:
        metrics = chat.setdefault("metrics", {})
        metrics.setdefault("bot_replies", {})

    # ----------------------------
    # Internal helpers
    # ----------------------------
    def _chat(self, state: Dict[str, Any], chat_id: int) -> Dict[str, Any]:
        chats = state.setdefault("chats", {})
        chat = chats.setdefault(str(chat_id), {})
        chat.setdefault("users", {})
        chat.setdefault("entitlement", None)
        chat.setdefault("metrics", {})
        self._ensure_chat_metrics(chat)
        return chat

    def _user(self, chat: Dict[str, Any], user_id: int) -> Dict[str, Any]:
        users = chat.setdefault("users", {})
        u = users.setdefault(str(user_id), {})
        u.setdefault("first_seen", iso_now())
        u.setdefault("last_seen", iso_now())
        u.setdefault("username", "")
        u.setdefault("name", "")
        u.setdefault("is_bot", False)
        u.setdefault("activity_days", {})
        u.setdefault("risk_score", 0)
        return u

    def _gen_id(self, prefix: str) -> str:
        return f"{prefix}_{secrets.token_hex(6)}"

    def _provider_enabled(self, state: Dict[str, Any], provider: str) -> bool:
        providers = state["config"].get("providers", {})
        return bool(providers.get(provider, {}).get("enabled", False))

    def _pricing(self, state: Dict[str, Any]) -> Dict[str, Any]:
        return state["config"]["pricing"]

    def _split_cfg(self, state: Dict[str, Any]) -> Dict[str, Any]:
        return state["config"]["split"]

    def _free_cfg(self, state: Dict[str, Any]) -> Dict[str, Any]:
        return state["config"]["free_promo"]

    def _today(self) -> str:
        return utcnow().date().isoformat()

    def _month_end(self, days: int = 30) -> str:
        return (utcnow() + timedelta(days=days)).isoformat()

    def _active_window_start(self, state: Dict[str, Any]) -> datetime:
        days = int(self._pricing(state)["active_window_days"])
        return utcnow() - timedelta(days=days)

    def _calc_active_users(self, state: Dict[str, Any], chat_id: int) -> List[int]:
        chat = self._chat(state, chat_id)
        threshold = int(self._pricing(state)["active_threshold_messages"])
        cutoff = self._active_window_start(state).date()
        active: List[int] = []

        for uid, user in chat["users"].items():
            if user.get("is_bot"):
                continue
            total = 0
            day_counts: Dict[str, Any] = user.get("activity_days", {})
            for day_str, cnt in day_counts.items():
                try:
                    d = datetime.fromisoformat(day_str).date()
                except Exception:
                    continue
                if d >= cutoff:
                    total += int(cnt)
            if total >= threshold:
                active.append(int(uid))

        active.sort()
        return active

    def _prune_chat_activity(self, state: Dict[str, Any], chat_id: int, save: bool = True) -> None:
        chat = self._chat(state, chat_id)
        keep_since = (utcnow() - timedelta(days=90)).date()
        dirty = False

        for _, user in chat.get("users", {}).items():
            day_counts = user.get("activity_days", {})
            keys = list(day_counts.keys())
            for day_str in keys:
                try:
                    d = datetime.fromisoformat(day_str).date()
                except Exception:
                    del day_counts[day_str]
                    dirty = True
                    continue
                if d < keep_since:
                    del day_counts[day_str]
                    dirty = True

        if dirty and save:
            self._save(state)

    def _record_abuse_flag(self, state: Dict[str, Any], chat_id: int, kind: str, details: str) -> None:
        state["abuse"]["flags"].append(
            {
                "chat_id": chat_id,
                "kind": kind,
                "details": details,
                "ts": iso_now(),
            }
        )
        if len(state["abuse"]["flags"]) > 500:
            state["abuse"]["flags"] = state["abuse"]["flags"][-500:]

    def _check_batch_limits(self, state: Dict[str, Any], chat_id: int) -> None:
        today = self._today()
        by_day = state["abuse"].setdefault("invoice_batches_per_day", {})
        today_map = by_day.setdefault(today, {})
        max_batches = int(self._split_cfg(state)["max_invoice_batches_per_chat_per_day"])

        used = int(today_map.get(str(chat_id), 0))
        if used >= max_batches:
            self._record_abuse_flag(
                state,
                chat_id,
                "batch_limit",
                f"invoice batch limit reached: {used}/{max_batches}",
            )
            raise BillingError("лимит создания платежных батчей на сегодня исчерпан")

    def _consume_batch_limit(self, state: Dict[str, Any], chat_id: int) -> None:
        today = self._today()
        by_day = state["abuse"].setdefault("invoice_batches_per_day", {})
        today_map = by_day.setdefault(today, {})
        today_map[str(chat_id)] = int(today_map.get(str(chat_id), 0)) + 1

    def _check_change_cooldown(self, state: Dict[str, Any], chat_id: int) -> None:
        cooldown_hours = int(self._split_cfg(state)["change_cooldown_hours"])
        last_raw = state["abuse"].setdefault("last_plan_change_ts", {}).get(str(chat_id))
        if not last_raw:
            return
        try:
            last = parse_iso(last_raw)
        except Exception:
            return
        if utcnow() < last + timedelta(hours=cooldown_hours):
            remain = (last + timedelta(hours=cooldown_hours) - utcnow()).total_seconds()
            hrs = max(1, int(remain // 3600))
            self._record_abuse_flag(
                state,
                chat_id,
                "cooldown",
                f"plan changed too frequently, wait ~{hrs}h",
            )
            raise BillingError(f"слишком частая смена платежной схемы, подожди примерно {hrs}ч")

    def _mark_plan_change(self, state: Dict[str, Any], chat_id: int) -> None:
        state["abuse"].setdefault("last_plan_change_ts", {})[str(chat_id)] = iso_now()

    def _split_amounts(self, total_rub: int, payer_ids: List[int]) -> Dict[int, int]:
        if not payer_ids:
            raise BillingError("нет плательщиков")
        base = total_rub // len(payer_ids)
        remainder = total_rub % len(payer_ids)
        out: Dict[int, int] = {}
        for idx, uid in enumerate(payer_ids):
            out[uid] = base + (1 if idx < remainder else 0)
        return out

    def _append_ledger(self, state: Dict[str, Any], item: Dict[str, Any]) -> None:
        ledger = state.setdefault("ledger", [])
        ledger.append(item)
        if len(ledger) > 3000:
            state["ledger"] = ledger[-3000:]

    def _create_entitlement(self, state: Dict[str, Any], chat_id: int, sub: Dict[str, Any]) -> None:
        chat = self._chat(state, chat_id)
        chat["entitlement"] = {
            "subscription_id": sub["subscription_id"],
            "status": sub["status"],
            "tier": sub["tier"],
            "mode": sub["mode"],
            "provider": sub["provider"],
            "starts_at": sub.get("activated_at"),
            "expires_at": sub.get("expires_at"),
            "features": sub.get("features", {}),
        }

    def _ref_program_for_user(self, state: Dict[str, Any], user_id: int) -> Optional[Dict[str, Any]]:
        attrs = state["referrals"].setdefault("user_attribution", {})
        data = attrs.get(str(user_id))
        if not data:
            return None
        try:
            expires_at = parse_iso(data["expires_at"])
            if utcnow() > expires_at:
                return None
        except Exception:
            return None
        pid = data.get("program_id")
        if not pid:
            return None
        return state["referrals"].setdefault("programs", {}).get(pid)

    # ----------------------------
    # Public API
    # ----------------------------
    def register_activity(
        self,
        chat_id: int,
        user_id: int,
        username: str,
        name: str,
        is_bot: bool,
        ts: Optional[datetime] = None,
    ) -> None:
        state = self._load()
        chat = self._chat(state, chat_id)
        user = self._user(chat, user_id)

        now = ts or utcnow()
        day = now.date().isoformat()

        user["username"] = username or user.get("username", "")
        user["name"] = name or user.get("name", "")
        user["is_bot"] = bool(is_bot)
        user["last_seen"] = now.isoformat()
        user.setdefault("activity_days", {})[day] = int(user["activity_days"].get(day, 0)) + 1

        self._prune_chat_activity(state, chat_id, save=False)
        self._save(state)

    def get_quote(
        self,
        chat_id: int,
        mode: str,
        provider: str,
        payer_count: Optional[int] = None,
        specific_payer_ids: Optional[List[int]] = None,
    ) -> QuoteResult:
        state = self._load()
        mode = mode.lower().strip()
        provider = provider.lower().strip()

        if mode not in {"owner", "split", "free"}:
            raise BillingError("неизвестный режим оплаты")

        if mode != "free" and not self._provider_enabled(state, provider):
            raise BillingError("платежный провайдер отключен")

        active = self._calc_active_users(state, chat_id)
        pricing = self._pricing(state)
        per_active = int(pricing["per_active_rub"])
        min_price = int(pricing["min_group_rub"])
        max_price = int(pricing["max_group_rub"])

        if mode == "free":
            total = 0
            payers: List[int] = []
        else:
            total = max(min_price, min(max_price, len(active) * per_active))
            if specific_payer_ids:
                payers = sorted(set(int(x) for x in specific_payer_ids if int(x) in active))
            else:
                if mode == "owner":
                    payers = []
                else:
                    count = payer_count if payer_count is not None else len(active)
                    count = max(1, min(len(active), int(count) if count else len(active)))
                    payers = active[:count]

        return QuoteResult(
            chat_id=chat_id,
            mode=mode,
            provider=provider,
            active_users=active,
            payer_ids=payers,
            total_rub=total,
            price_per_active_rub=per_active,
            min_price_rub=min_price,
            max_price_rub=max_price,
            activation_ratio=float(self._split_cfg(state)["activation_ratio"]),
        )

    def create_subscription_cycle(
        self,
        chat_id: int,
        initiated_by_user_id: int,
        mode: str,
        provider: str = "stars",
        payer_count: Optional[int] = None,
        specific_payer_ids: Optional[List[int]] = None,
        tier: str = "group_standard",
    ) -> Dict[str, Any]:
        state = self._load()
        mode = mode.lower().strip()
        provider = provider.lower().strip()

        self._check_batch_limits(state, chat_id)
        self._check_change_cooldown(state, chat_id)

        quote = self.get_quote(
            chat_id=chat_id,
            mode=mode,
            provider=provider,
            payer_count=payer_count,
            specific_payer_ids=specific_payer_ids,
        )

        if mode in {"owner", "split"} and quote.total_rub <= 0:
            raise BillingError("не удалось рассчитать сумму")

        sub_id = self._gen_id("sub")
        sub = {
            "subscription_id": sub_id,
            "chat_id": chat_id,
            "initiated_by": initiated_by_user_id,
            "mode": mode,
            "provider": provider,
            "tier": tier,
            "status": "pending_payment",
            "total_rub": quote.total_rub,
            "active_users_snapshot": quote.active_users,
            "payer_ids": [],
            "invoice_ids": [],
            "activation_ratio": quote.activation_ratio,
            "created_at": iso_now(),
            "activated_at": None,
            "expires_at": None,
            "features": self.tier_features(tier, mode),
        }

        if mode == "free":
            sub["status"] = "active"
            sub["activated_at"] = iso_now()
            sub["expires_at"] = self._month_end(30)
            state["subscriptions"][sub_id] = sub
            self._create_entitlement(state, chat_id, sub)
            self._append_ledger(
                state,
                {
                    "type": "subscription_activated",
                    "subscription_id": sub_id,
                    "chat_id": chat_id,
                    "mode": mode,
                    "provider": provider,
                    "amount_rub": 0,
                    "ts": iso_now(),
                },
            )
            self._consume_batch_limit(state, chat_id)
            self._mark_plan_change(state, chat_id)
            self._save(state)
            return {
                "subscription": sub,
                "invoices": [],
                "quote": quote.__dict__,
            }

        if mode == "owner":
            payers = [int(initiated_by_user_id)]
        else:
            payers = quote.payer_ids
            if not payers:
                raise BillingError("для split не найдено ни одного активного плательщика")

        amounts = self._split_amounts(quote.total_rub, payers)

        invoices = []
        for uid in payers:
            inv_id = self._gen_id("inv")
            invoice = {
                "invoice_id": inv_id,
                "subscription_id": sub_id,
                "chat_id": chat_id,
                "provider": provider,
                "payer_user_id": uid,
                "amount_rub": int(amounts[uid]),
                "status": "pending",
                "created_at": iso_now(),
                "paid_at": None,
                "external_payment_id": None,
            }
            state["invoices"][inv_id] = invoice
            invoices.append(invoice)
            sub["invoice_ids"].append(inv_id)

        sub["payer_ids"] = payers
        state["subscriptions"][sub_id] = sub
        self._append_ledger(
            state,
            {
                "type": "subscription_created",
                "subscription_id": sub_id,
                "chat_id": chat_id,
                "mode": mode,
                "provider": provider,
                "amount_rub": quote.total_rub,
                "payer_count": len(payers),
                "ts": iso_now(),
            },
        )

        self._consume_batch_limit(state, chat_id)
        self._mark_plan_change(state, chat_id)
        self._save(state)
        return {
            "subscription": sub,
            "invoices": invoices,
            "quote": quote.__dict__,
        }

    def pay_invoice_mock(
        self,
        invoice_id: str,
        paid_by_user_id: int,
        provider: Optional[str] = None,
        external_payment_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        state = self._load()
        invoice = state["invoices"].get(invoice_id)
        if not invoice:
            raise BillingError("инвойс не найден")

        if invoice["status"] == "paid":
            return {
                "invoice": invoice,
                "subscription": state["subscriptions"].get(invoice["subscription_id"]),
                "already_paid": True,
            }

        if int(invoice["payer_user_id"]) != int(paid_by_user_id):
            self._record_abuse_flag(
                state,
                int(invoice["chat_id"]),
                "payer_mismatch",
                f"invoice={invoice_id}, expected={invoice['payer_user_id']}, got={paid_by_user_id}",
            )
            self._save(state)
            raise BillingError("этот инвойс выставлен другому пользователю")

        if provider and provider != invoice["provider"]:
            raise BillingError("провайдер оплаты не совпадает")

        invoice["status"] = "paid"
        invoice["paid_at"] = iso_now()
        invoice["external_payment_id"] = external_payment_id or self._gen_id("pay")

        sub = state["subscriptions"].get(invoice["subscription_id"])
        if not sub:
            raise BillingError("подписка для инвойса не найдена")

        self._append_ledger(
            state,
            {
                "type": "invoice_paid",
                "invoice_id": invoice_id,
                "subscription_id": sub["subscription_id"],
                "chat_id": sub["chat_id"],
                "user_id": paid_by_user_id,
                "amount_rub": invoice["amount_rub"],
                "provider": invoice["provider"],
                "ts": iso_now(),
            },
        )

        self._apply_referral_commission_if_any(state, sub, invoice)
        self._try_activate_subscription(state, sub["subscription_id"])
        self._save(state)
        return {
            "invoice": invoice,
            "subscription": state["subscriptions"].get(sub["subscription_id"]),
            "already_paid": False,
        }

    def _apply_referral_commission_if_any(self, state: Dict[str, Any], sub: Dict[str, Any], invoice: Dict[str, Any]) -> None:
        program = self._ref_program_for_user(state, int(sub["initiated_by"]))
        if not program:
            return
        pct = float(program.get("commission_pct", 0))
        if pct <= 0:
            return

        affiliate_user_id = int(program["owner_user_id"])
        reward = int(round(int(invoice["amount_rub"]) * pct / 100.0))
        balances = state["referrals"].setdefault("affiliate_balances", {})
        balances[str(affiliate_user_id)] = int(balances.get(str(affiliate_user_id), 0)) + reward

        event = {
            "type": "affiliate_reward",
            "affiliate_user_id": affiliate_user_id,
            "source_user_id": sub["initiated_by"],
            "subscription_id": sub["subscription_id"],
            "invoice_id": invoice["invoice_id"],
            "reward_rub": reward,
            "commission_pct": pct,
            "ts": iso_now(),
        }
        state["referrals"].setdefault("events", []).append(event)
        if len(state["referrals"]["events"]) > 2000:
            state["referrals"]["events"] = state["referrals"]["events"][-2000:]

    def _try_activate_subscription(self, state: Dict[str, Any], subscription_id: str) -> None:
        sub = state["subscriptions"].get(subscription_id)
        if not sub:
            return

        if sub.get("status") == "active":
            return

        invoice_ids = list(sub.get("invoice_ids", []))
        if not invoice_ids:
            return

        paid = 0
        total = 0
        for inv_id in invoice_ids:
            inv = state["invoices"].get(inv_id)
            if not inv:
                continue
            amount = int(inv.get("amount_rub", 0))
            total += amount
            if inv.get("status") == "paid":
                paid += amount

        ratio = (paid / total) if total else 0.0
        required = float(sub.get("activation_ratio", 1.0))
        if ratio + 1e-9 < required:
            return

        sub["status"] = "active"
        sub["activated_at"] = iso_now()
        sub["expires_at"] = self._month_end(30)
        self._create_entitlement(state, int(sub["chat_id"]), sub)
        self._append_ledger(
            state,
            {
                "type": "subscription_activated",
                "subscription_id": sub["subscription_id"],
                "chat_id": sub["chat_id"],
                "mode": sub["mode"],
                "provider": sub["provider"],
                "amount_rub": total,
                "ts": iso_now(),
            },
        )

    def get_chat_entitlement(self, chat_id: int) -> Optional[Dict[str, Any]]:
        state = self._load()
        chat = self._chat(state, chat_id)
        ent = chat.get("entitlement")
        if not ent:
            return None
        exp = ent.get("expires_at")
        if exp:
            try:
                if utcnow() > parse_iso(exp):
                    ent["status"] = "expired"
                    self._save(state)
            except Exception:
                pass
        return ent

    def should_apply_free_watermark(self, chat_id: int) -> Tuple[bool, str]:
        state = self._load()
        free_cfg = self._free_cfg(state)
        if not bool(free_cfg.get("enabled", False)):
            return (False, "")

        chat = self._chat(state, chat_id)
        ent = chat.get("entitlement")
        if ent and ent.get("status") == "active" and ent.get("tier") != "free_promo":
            return (False, "")

        today = self._today()
        metrics = chat.setdefault("metrics", {}).setdefault("bot_replies", {})
        metrics[today] = int(metrics.get(today, 0)) + 1

        daily_quota = int(free_cfg.get("daily_quota", 30))
        every_n = max(1, int(free_cfg.get("watermark_every_n", 4)))
        water = str(free_cfg.get("watermark_text", "promo: upgrade"))

        count = int(metrics[today])
        self._save(state)

        if count > daily_quota:
            return (True, "лимит free-тарифа на сегодня, апгрейднись")
        if count % every_n == 0:
            return (True, water)
        return (False, "")

    def create_affiliate_program(
        self,
        owner_user_id: int,
        commission_pct: Optional[int] = None,
        duration_months: Optional[int] = None,
    ) -> Dict[str, Any]:
        state = self._load()
        aff_cfg = state["config"]["affiliate"]
        max_pct = int(aff_cfg["max_commission_pct"])
        pct = int(commission_pct if commission_pct is not None else aff_cfg["default_commission_pct"])
        months = int(duration_months if duration_months is not None else aff_cfg["default_duration_months"])

        if pct < 1 or pct > max_pct:
            raise BillingError(f"commission должен быть 1..{max_pct}%")
        if months < 1 or months > 36:
            raise BillingError("duration должен быть 1..36 месяцев")

        pid = self._gen_id("aff")
        code = secrets.token_urlsafe(6).replace("-", "").replace("_", "")[:10].upper()

        program = {
            "program_id": pid,
            "owner_user_id": int(owner_user_id),
            "commission_pct": pct,
            "duration_months": months,
            "code": code,
            "status": "active",
            "created_at": iso_now(),
        }

        state["referrals"]["programs"][pid] = program
        state["referrals"]["codes"][code] = pid
        self._save(state)
        return program

    def apply_referral_code(self, user_id: int, code: str) -> Dict[str, Any]:
        state = self._load()
        c = code.strip().upper()
        pid = state["referrals"].setdefault("codes", {}).get(c)
        if not pid:
            raise BillingError("реферальный код не найден")

        program = state["referrals"].setdefault("programs", {}).get(pid)
        if not program or program.get("status") != "active":
            raise BillingError("реферальная программа неактивна")

        months = int(program.get("duration_months", 3))
        expires_at = (utcnow() + timedelta(days=30 * months)).isoformat()

        state["referrals"].setdefault("user_attribution", {})[str(user_id)] = {
            "program_id": pid,
            "code": c,
            "set_at": iso_now(),
            "expires_at": expires_at,
        }

        state["referrals"].setdefault("events", []).append(
            {
                "type": "referral_attribution",
                "user_id": user_id,
                "program_id": pid,
                "code": c,
                "ts": iso_now(),
            }
        )
        self._save(state)
        return {
            "user_id": user_id,
            "program_id": pid,
            "code": c,
            "expires_at": expires_at,
        }

    def get_affiliate_balance(self, user_id: int) -> int:
        state = self._load()
        return int(state["referrals"].setdefault("affiliate_balances", {}).get(str(user_id), 0))

    def get_chat_activity_summary(self, chat_id: int) -> Dict[str, Any]:
        state = self._load()
        chat = self._chat(state, chat_id)
        active = self._calc_active_users(state, chat_id)

        users = chat.get("users", {})
        items: List[Dict[str, Any]] = []
        for uid_str, u in users.items():
            uid = int(uid_str)
            last_seen = u.get("last_seen")
            cnt = 0
            for _, c in u.get("activity_days", {}).items():
                cnt += int(c)
            items.append(
                {
                    "user_id": uid,
                    "name": u.get("name") or u.get("username") or str(uid),
                    "username": u.get("username", ""),
                    "is_bot": bool(u.get("is_bot", False)),
                    "messages_90d": cnt,
                    "is_active": uid in active,
                    "last_seen": last_seen,
                }
            )

        items.sort(key=lambda x: (-int(x["messages_90d"]), x["name"]))

        ent = chat.get("entitlement")
        return {
            "chat_id": chat_id,
            "active_users": active,
            "active_count": len(active),
            "users": items,
            "entitlement": ent,
        }

    def list_chat_invoices(self, chat_id: int, limit: int = 20) -> List[Dict[str, Any]]:
        state = self._load()
        out = [inv for inv in state.get("invoices", {}).values() if int(inv.get("chat_id", 0)) == int(chat_id)]
        out.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return out[:limit]

    def get_abuse_report(self, chat_id: Optional[int] = None, limit: int = 20) -> Dict[str, Any]:
        state = self._load()
        flags = state.get("abuse", {}).get("flags", [])
        if chat_id is not None:
            flags = [f for f in flags if int(f.get("chat_id", 0)) == int(chat_id)]
        flags = flags[-limit:]

        today = self._today()
        today_counts = state.get("abuse", {}).get("invoice_batches_per_day", {}).get(today, {})
        return {
            "today": today,
            "invoice_batches_today": today_counts,
            "flags": flags,
        }

    def tier_features(self, tier: str, mode: str) -> Dict[str, Any]:
        # mock but future-ready feature flags
        if tier == "free_promo" or mode == "free":
            return {
                "max_daily_replies": 30,
                "persona_modes": ["default", "chill"],
                "meme_generator": False,
                "voice_circles": False,
                "mini_games": False,
                "watermark": True,
            }
        if tier == "group_plus":
            return {
                "max_daily_replies": 3000,
                "persona_modes": ["default", "savage", "chill", "poet", "npc", "quotes"],
                "meme_generator": True,
                "voice_circles": True,
                "mini_games": True,
                "watermark": False,
            }
        return {
            "max_daily_replies": 1200,
            "persona_modes": ["default", "savage", "chill", "npc", "quotes"],
            "meme_generator": True,
            "voice_circles": False,
            "mini_games": False,
            "watermark": False,
        }
