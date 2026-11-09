"""Identical simulated I/O cost per secondary action in both modes."""
import time


def perform_action(repo, task_type, booking_id, recipient, delay_ms, mode):
    if delay_ms:
        time.sleep(delay_ms / 1000)
    if task_type == 'audit':
        repo.create_audit_log(booking_id, f'booking_created_{mode}')
    elif task_type == 'notification':
        repo.create_notification(booking_id, recipient)
    else:
        raise ValueError('Unknown task type')
