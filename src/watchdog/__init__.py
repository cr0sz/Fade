"""Infrastructure watchdog + daily Telegram digest (Sprint 49).

Covers INFRASTRUCTURE health only (dead containers, silent data feeds, dead
API / websocket). Signal-quality alerts remain the job of
src/engines/validation_alert_engine.py — this service does not overlap it.
"""
