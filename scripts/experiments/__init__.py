"""Fault-injection experiments over the real scheduler with a fake harness.

Every module here drives production code (``SubagentManager`` admission, the
``taskq`` store, the adaptive controller, the dependency coordinator, the
recovery ladder and the in-process ``SpawnGate``/``HostBudget``) against a fake
worker and a virtual clock. Nothing spawns kiro-cli or opens a socket.
"""
