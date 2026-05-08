# apps/certificate_approver/scheduler.py

_auto_approve_enabled = False

def get_auto_approve_enabled() -> bool:
    global _auto_approve_enabled
    return _auto_approve_enabled

def set_auto_approve_enabled(enabled: bool):
    global _auto_approve_enabled
    _auto_approve_enabled = enabled

# We can implement background polling later if the user turns it on.
# Currently, it is strictly manual to analyze the cert with AI and manually approve/deny.
