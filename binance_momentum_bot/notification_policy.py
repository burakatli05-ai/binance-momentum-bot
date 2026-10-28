"""Delivery-only policy. Does not change detection, recording or order handling."""
import os


def early_signal_notifications_enabled():
    # User authorized muting EARLY signal cards. Real execution/position/risk
    # notifications must never use this switch.
    return os.getenv('EARLY_SIGNAL_TELEGRAM_ENABLED', '0').strip().lower() in (
        '1', 'true', 'yes', 'on')
