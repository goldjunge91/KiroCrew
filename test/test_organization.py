"""Organization boundaries, durable delegation and concurrent admission."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import pytest

from kiro_crew.organization import (
    DEFAULT_STAFFING,
    OWNER,
    OrganizationError,
    OrganizationStore,
)


@pytest.fixture
def team(tmp_path):
    store = OrganizationStore(tmp_path / "organizations" / "organization.sqlite3")
    conductor = store.enroll(
        OWNER,
        name="Conductor",
        memory_store="private-conductor",
        role="conductor",
        manager_id=None,
    )
    manager = store.enroll(
        OWNER,
        name="Manager",
        memory_store="private-manager",
        role="manager",
        manager_id=conductor,
    )
    engineer = store.enroll(
        OWNER,
        name="Engineer",
        memory_store="private-engineer",
        role="engineer",
        manager_id=manager,
    )
    researcher = store.enroll(
        OWNER,
        name="Researcher",
        memory_store="private-researcher",
        role="researcher",
        manager_id=conductor,
    )
    return store, conductor, manager, engineer, researcher


def enable(store, concurrency=3):
    store.configure(
        OWNER,
        revision=store.snapshot()["settings"]["revision"],
        concurrency=concurrency,
        enabled=True,
        staffing=deepcopy(DEFAULT_STAFFING),
    )


@pytest.mark.parametrize("member_index", (1, 2, 3, 4))
def test_owner_chat_task_is_for_the_calling_member_with_existing_role_rules(team, member_index):
    store, *members = team
    member = members[member_index - 1]
    task_id = store.start_task_from_chat(
        member, "owner-turn", title="Requested in chat", acceptance="Evidence checked"
    )
    task = next(t for t in store.snapshot()["tasks"] if t["id"] == task_id)
    assert (task["sender"], task["recipient"], task["parent_id"]) == (OWNER, member, None)
    if member_index in (1, 2):
        with pytest.raises(OrganizationError) as denied:
            store.report(member, task_id, "done", "No delegation")
        assert denied.value.code == "delegation_required"
    else:
        store.report(member, task_id, "done", "Evidence")
        assert store.snapshot()["tasks"][0]["state"] == "review"
        with pytest.raises(OrganizationError) as denied:
            store.review(member, task_id, "accept", "Self approval")
        assert denied.value.code == "review_denied"


def test_chat_task_can_immediately_parent_a_delegation(team):
    store, conductor, manager, *_ = team
    task = store.start_task_from_chat(
        conductor, "owner-turn", title="Build", acceptance="Checked implementation"
    )
    child = store.assign(conductor, manager, parent_id=task, title="Implement", acceptance="Tests")
    assert next(t for t in store.snapshot()["tasks"] if t["id"] == child)["parent_id"] == task


def test_intermediate_progress_wakes_the_manager_without_completing_the_assignment(team):
    store, conductor, manager, *_ = team
    enable(store)
    root = store.assign(OWNER, conductor, title="Build", acceptance="Working engine and match")
    child = store.assign(
        conductor, manager, parent_id=root, title="Implement", acceptance="Engine and match"
    )
    while run := store.claim_run():
        store.finish_run(run["id"])

    store.report(manager, child, "progress", "Engine built; review the milestone before the match")
    store.report(manager, child, "progress", "The milestone evidence now includes perft results")
    wake = store.claim_run()
    assert wake is not None
    assert (wake["member_id"], wake["reason"]) == (conductor, "report")
    assert store.claim_run() is None
    task = next(t for t in store.snapshot()["tasks"] if t["id"] == child)
    assert task["state"] == "working"
    assert "perft results" in task["report"]
    store.finish_run(wake["id"])

    store.report(conductor, root, "progress", "Reviewing the integration milestone")
    assert store.claim_run() is None  # The owner is not an automated member.


def test_repeated_concurrent_chat_registration_is_one_durable_task(team):
    store, conductor, manager, *_ = team

    def register(_):
        return OrganizationStore(store.path).start_task_from_chat(
            conductor, "owner-turn", title="Build", acceptance="Tests"
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        ids = list(pool.map(register, range(8), timeout=15))
    assert len(set(ids)) == 1
    assert len(store.snapshot()["tasks"]) == 1
    assert len(store.snapshot()["runs"]) == 1
    with pytest.raises(OrganizationError) as denied:
        store.start_task_from_chat(manager, "owner-turn", title="Borrow", acceptance="Spoof")
    assert denied.value.code == "owner_request_mismatch"
    store.start_task_from_chat(conductor, "another-turn", title="Another", acceptance="Tests")
    assert len(store.snapshot()["tasks"]) == 2


def test_hiring_does_not_reuse_a_report_with_a_pending_message_turn(team):
    store, conductor, manager, engineer, researcher = team
    store.message(manager, engineer, "Check your private project convention")
    reservation = store.reserve_hire(manager, "engineer")
    assert reservation["member_id"] == ""
    assert reservation["reservation"]


def test_member_identity_and_records_survive_a_new_store_instance(team):
    store, conductor, manager, engineer, researcher = team
    task = store.assign(OWNER, conductor, title="Build a feature", acceptance="Tests pass")
    reopened = OrganizationStore(store.path)
    assert reopened.member_for_store("private-engineer")["id"] == engineer
    assert reopened.snapshot()["tasks"][0]["id"] == task
    assert len(reopened.snapshot()["members"]) == 4
    assert all("memory_store" not in member for member in reopened.snapshot()["members"])


def test_only_one_conductor_root_and_unique_private_identity(team):
    store, conductor, manager, engineer, researcher = team
    with pytest.raises(OrganizationError, match="one conductor"):
        store.enroll(OWNER, name="Second", memory_store="new", role="conductor", manager_id=None)
    with pytest.raises(OrganizationError) as denied:
        store.enroll(
            OWNER,
            name="Imposter",
            memory_store="private-engineer",
            role="engineer",
            manager_id=manager,
        )
    assert denied.value.code == "member_exists"
    assert len(store.snapshot()["members"]) == 4


def test_members_see_only_their_reporting_line_and_own_correspondence(team):
    store, conductor, manager, engineer, researcher = team
    store.message(OWNER, researcher, "Owner research note")
    store.message(conductor, manager, "Manager note")
    store.message(manager, engineer, "Engineering note")
    view = store.snapshot(engineer)
    assert {member["id"] for member in view["members"]} == {manager, engineer}
    assert [message["text"] for message in view["messages"]] == ["Engineering note"]
    assert view["actor"] == engineer


@pytest.mark.parametrize("source,target", [(3, 4), (3, 1), (4, 2), (3, OWNER)])
def test_non_adjacent_messages_are_refused_without_a_write(team, source, target):
    store = team[0]
    actor = team[source]
    recipient = OWNER if target == OWNER else team[target]
    revision = store.snapshot()["settings"]["revision"]
    with pytest.raises(OrganizationError) as denied:
        store.message(actor, recipient, "Bypass my manager")
    assert denied.value.code == "outside_reporting_line"
    assert not store.snapshot()["messages"]
    assert store.snapshot()["settings"]["revision"] == revision


def test_owner_can_contact_anyone_but_cannot_make_worker_self_accept(team):
    store, conductor, manager, engineer, researcher = team
    store.message(OWNER, engineer, "Please investigate")
    task = store.assign(OWNER, engineer, title="Investigate", acceptance="Evidence")
    store.report(engineer, task, "done", "Evidence attached")
    with pytest.raises(OrganizationError) as denied:
        store.review(engineer, task, "accept", "I approve myself")
    assert denied.value.code == "review_denied"
    assert store.snapshot()["tasks"][0]["state"] == "review"
    store.review(OWNER, task, "accept", "Evidence checked")
    assert store.snapshot()["tasks"][0]["state"] == "accepted"


def test_real_hierarchy_requires_delegation_and_separate_acceptance(team):
    store, conductor, manager, engineer, researcher = team
    goal = store.assign(OWNER, conductor, title="Feature", acceptance="Works")
    with pytest.raises(OrganizationError) as denied:
        store.report(conductor, goal, "done", "I did it myself")
    assert denied.value.code == "delegation_required"
    work = store.assign(conductor, manager, title="Build", acceptance="Tests", parent_id=goal)
    leaf = store.assign(manager, engineer, title="Implement", acceptance="Tests", parent_id=work)
    store.report(engineer, leaf, "done", "Patch and test evidence")
    with pytest.raises(OrganizationError) as denied:
        store.report(manager, work, "done", "Assume success")
    assert denied.value.code == "delegation_required"
    store.review(manager, leaf, "revise", "Add the failure case")
    assert next(t for t in store.snapshot()["tasks"] if t["id"] == leaf)["state"] == "working"
    store.report(engineer, leaf, "done", "Failure case verified")
    store.review(manager, leaf, "accept", "Checked both cases")
    store.report(manager, work, "done", "Engineering accepted")
    store.review(conductor, work, "accept", "Acceptance met")
    store.report(conductor, goal, "done", "Ready for owner")
    assert next(t for t in store.snapshot()["tasks"] if t["id"] == goal)["state"] == "review"
    store.review(OWNER, goal, "accept", "Tried the feature")
    assert all(t["state"] == "accepted" for t in store.snapshot()["tasks"])


def test_cannot_delegate_to_sibling_or_report_on_another_members_task(team):
    store, conductor, manager, engineer, researcher = team
    goal = store.assign(OWNER, conductor, title="Feature", acceptance="Works")
    with pytest.raises(OrganizationError) as denied:
        store.assign(conductor, engineer, title="Skip manager", acceptance="Done", parent_id=goal)
    assert denied.value.code == "delegation_denied"
    with pytest.raises(OrganizationError) as denied:
        store.report(manager, goal, "done", "Spoof conductor")
    assert denied.value.code == "report_denied"
    with pytest.raises(OrganizationError) as denied:
        store.assign(manager, engineer, title="Borrow goal", acceptance="Done", parent_id=goal)
    assert denied.value.code == "parent_not_owned"


def test_staffing_reuses_idle_report_before_reserving_a_new_identity(team):
    store, conductor, manager, engineer, researcher = team
    result = store.reserve_hire(manager, "engineer")
    assert result == {"member_id": engineer, "reservation": ""}
    store.assign(OWNER, engineer, title="Busy", acceptance="Finish")
    reservation = store.reserve_hire(manager, "engineer")
    assert reservation["reservation"] and not reservation["member_id"]
    new_id = store.enroll(
        manager,
        name="Engineer 2",
        memory_store="private-engineer-2",
        role="engineer",
        manager_id=manager,
        reservation=reservation["reservation"],
    )
    assert new_id != engineer
    assert store.member_for_store("private-engineer")["id"] == engineer
    with pytest.raises(OrganizationError) as denied:
        store.enroll(
            manager,
            name="Engineer 3",
            memory_store="private-engineer-3",
            role="engineer",
            manager_id=manager,
            reservation=reservation["reservation"],
        )
    assert denied.value.code == "invalid_reservation"


def test_two_managers_have_independent_headcount_limits(team):
    store, conductor, manager, engineer, researcher = team
    manager2 = store.enroll(
        OWNER,
        name="Manager 2",
        memory_store="private-manager-2",
        role="manager",
        manager_id=conductor,
    )
    for number in range(2):
        store.enroll(
            OWNER,
            name=f"Additional {number}",
            memory_store=f"private-additional-{number}",
            role="engineer",
            manager_id=manager,
        )
    with pytest.raises(OrganizationError) as denied:
        store.enroll(
            OWNER,
            name="Overflow",
            memory_store="private-overflow",
            role="engineer",
            manager_id=manager,
        )
    assert denied.value.code == "staffing_limit"
    for number in range(3):
        store.enroll(
            OWNER,
            name=f"Second team {number}",
            memory_store=f"private-second-{number}",
            role="engineer",
            manager_id=manager2,
        )
    assert sum(member["role"] == "engineer" for member in store.snapshot()["members"]) == 6


def test_atomic_hiring_reservations_cannot_oversubscribe(team):
    store, conductor, manager, engineer, researcher = team
    store.assign(OWNER, engineer, title="Occupied", acceptance="Finish")

    def reserve(_):
        try:
            return OrganizationStore(store.path).reserve_hire(manager, "engineer")
        except OrganizationError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(reserve, range(5)))
    assert len([result for result in results if isinstance(result, dict)]) == 2
    assert results.count("staffing_limit") == 3


def test_hiring_reservation_cannot_be_transferred_to_another_manager(team):
    store, conductor, manager, engineer, researcher = team
    store.assign(OWNER, engineer, title="Occupied", acceptance="Finish")
    reservation = store.reserve_hire(manager, "engineer")["reservation"]
    with pytest.raises(OrganizationError) as denied:
        store.enroll(
            conductor,
            name="Forged",
            memory_store="private-forged",
            role="engineer",
            manager_id=conductor,
            reservation=reservation,
        )
    assert denied.value.code == "invalid_reservation"
    store.abandon_hire(reservation)
    assert store.reserve_hire(manager, "engineer")["reservation"]


def test_owner_settings_use_revision_and_reject_impossible_staffing(team):
    store, conductor, manager, engineer, researcher = team
    revision = store.snapshot()["settings"]["revision"]
    enable(store)
    with pytest.raises(OrganizationError) as denied:
        store.configure(
            OWNER,
            revision=revision,
            concurrency=2,
            enabled=True,
            staffing=deepcopy(DEFAULT_STAFFING),
        )
    assert denied.value.code == "revision_conflict"
    with pytest.raises(OrganizationError) as denied:
        store.configure(
            manager,
            revision=store.snapshot()["settings"]["revision"],
            concurrency=2,
            enabled=True,
            staffing=deepcopy(DEFAULT_STAFFING),
        )
    assert denied.value.code == "owner_only"
    limits = deepcopy(DEFAULT_STAFFING)
    limits["manager"]["engineer"] = 0
    with pytest.raises(OrganizationError) as denied:
        store.configure(
            OWNER,
            revision=store.snapshot()["settings"]["revision"],
            concurrency=2,
            enabled=True,
            staffing=limits,
        )
    assert denied.value.code == "staffing_in_use"
    assert store.snapshot()["settings"]["concurrency"] == 3


def test_cannot_make_a_reporting_cycle_or_reassign_active_work(team):
    store, conductor, manager, engineer, researcher = team
    with pytest.raises(OrganizationError) as denied:
        store.move_member(OWNER, manager, engineer)
    assert denied.value.code == "reporting_cycle"
    manager2 = store.enroll(
        OWNER,
        name="Manager 2",
        memory_store="private-manager-2",
        role="manager",
        manager_id=conductor,
    )
    store.move_member(OWNER, engineer, manager2)
    assert store.member_for_store("private-engineer")["manager_id"] == manager2
    store.assign(OWNER, engineer, title="Busy", acceptance="Finish")
    with pytest.raises(OrganizationError) as denied:
        store.move_member(OWNER, engineer, manager)
    assert denied.value.code == "member_busy"


def test_retirement_retains_private_identity_and_history(team):
    store, conductor, manager, engineer, researcher = team
    store.retire(OWNER, engineer)
    identity = store.member_for_store("private-engineer")
    assert identity["id"] == engineer and identity["state"] == "retired"
    with pytest.raises(OrganizationError) as denied:
        store.message(engineer, manager, "I still work here")
    assert denied.value.code == "member_retired"
    with pytest.raises(OrganizationError) as denied:
        store.enroll(
            OWNER,
            name="Engineer",
            memory_store="private-successor",
            role="engineer",
            manager_id=manager,
        )
    assert denied.value.code == "member_exists"


@pytest.mark.parametrize("succeeded", [True, False])
def test_manager_cannot_retire_between_report_enrollment_and_publication(team, succeeded):
    store, _, manager, engineer, _ = team
    store.retire(OWNER, engineer)
    child = store.enroll(
        OWNER,
        name="Pending engineer",
        memory_store="pending-engineer",
        role="engineer",
        manager_id=manager,
        provisioning=True,
    )
    # This is the interleaving while the config writer publishes the report.
    with pytest.raises(OrganizationError) as denied:
        store.retire(OWNER, manager)
    assert denied.value.code == "has_reports"
    store.finish_provisioning(child, succeeded=succeeded)
    members = {member["id"]: member for member in store.snapshot()["members"]}
    assert members[manager]["state"] == "active"
    assert members[child]["state"] == ("active" if succeeded else "failed")
    if succeeded:
        store.retire(OWNER, child)
    store.retire(OWNER, manager)


def test_snapshot_retains_all_open_assignments_beyond_terminal_history_limit(team):
    store, _, _, engineer, researcher = team
    old = store.assign(OWNER, engineer, title="Old open work", acceptance="Evidence")
    for index in range(205):
        task = store.assign(OWNER, engineer, title=f"Closed {index}", acceptance="Evidence")
        store.review(OWNER, task, "cancel", "No longer needed")
    pending = {
        store.assign(OWNER, engineer, title=f"Open {index}", acceptance="Evidence")
        for index in range(201)
    } | {old}
    private = store.assign(OWNER, researcher, title="Other work", acceptance="Evidence")
    for actor in (OWNER, engineer):
        tasks = store.snapshot(actor)["tasks"]
        assert pending <= {task["id"] for task in tasks}
        assert sum(task["state"] == "cancelled" for task in tasks) == 200
        assert (private in {task["id"] for task in tasks}) is (actor == OWNER)
    store.report(engineer, old, "done", "Evidence delivered")
    store.review(OWNER, old, "accept", "Reviewed")


def test_concurrency_is_separate_from_team_size_and_only_one_turn_per_member(team):
    store, conductor, manager, engineer, researcher = team
    store.message(OWNER, engineer, "First")
    store.message(OWNER, engineer, "Second")
    store.message(OWNER, researcher, "Research")
    assert store.claim_run() is None  # Organization starts paused.
    enable(store, concurrency=1)
    run = store.claim_run()
    assert run["member_id"] == engineer
    store.message(OWNER, engineer, "During running turn")
    assert store.claim_run() is None
    assert len(store.snapshot()["members"]) == 4
    store.finish_run(run["id"])
    next_run = store.claim_run()
    assert next_run["member_id"] == researcher
    store.finish_run(next_run["id"], "Provider unavailable")
    assert store.claim_run()["member_id"] == engineer


def test_concurrent_claims_obey_global_capacity(team):
    store, conductor, manager, engineer, researcher = team
    enable(store, concurrency=2)
    for member in team[1:]:
        store.message(OWNER, member, "Wake")

    def claim(_):
        return OrganizationStore(store.path).claim_run()

    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(claim, range(5)))
    claimed = [result for result in results if result is not None]
    assert len(claimed) == 2
    assert len({run["member_id"] for run in claimed}) == 2


def test_busy_owner_conversation_does_not_starve_other_members(team):
    store, conductor, manager, engineer, researcher = team
    enable(store, concurrency=1)
    store.message(OWNER, engineer, "First in queue")
    store.message(OWNER, researcher, "Independent work")
    run = store.claim_run(busy_names=("Engineer",))
    assert run["member_id"] == researcher
    store.finish_run(run["id"])
    assert store.claim_run()["member_id"] == engineer


def test_retirement_cancels_a_leftover_wake_after_work_was_cancelled(team):
    store, conductor, manager, engineer, researcher = team
    assignment = store.assign(OWNER, engineer, title="No longer needed", acceptance="A result")
    store.review(OWNER, assignment, "cancel", "Requirements changed")
    store.retire(OWNER, engineer)
    assert store.snapshot()["runs"][0]["state"] == "cancelled"
    enable(store)
    assert store.claim_run() is None


def test_failed_report_turn_wakes_manager_without_exposing_private_history(team):
    store, conductor, manager, engineer, researcher = team
    enable(store)
    goal = store.assign(OWNER, manager, title="Implement", acceptance="Evidence")
    manager_run = store.claim_run()
    store.assign(manager, engineer, title="Code", acceptance="Tests", parent_id=goal)
    store.finish_run(manager_run["id"])
    worker_run = store.claim_run()
    store.finish_run(worker_run["id"], "Provider failed during preparation")
    parent_view = store.snapshot(manager)
    assert any(
        run["id"] == worker_run["id"] and run["state"] == "failed" for run in parent_view["runs"]
    )
    assert not store.snapshot(researcher)["runs"]
    assert store.claim_run()["member_id"] == manager


def test_repeated_failed_retries_require_owner_intervention(team):
    store, conductor, manager, engineer, researcher = team
    enable(store)
    for _ in range(3):
        store.retry(engineer, engineer)
        run = store.claim_run()
        store.finish_run(run["id"], "Unavailable")
    with pytest.raises(OrganizationError) as denied:
        store.retry(manager, engineer)
    assert denied.value.code == "retry_limit"
    store.retry(OWNER, engineer)
    assert store.claim_run()["member_id"] == engineer


def test_restart_does_not_silently_replay_in_flight_side_effects(team):
    store, conductor, manager, engineer, researcher = team
    enable(store)
    store.message(OWNER, engineer, "Wake")
    running = store.claim_run()
    restarted = OrganizationStore(store.path)
    assert restarted.recover_runs() == 1
    assert restarted.claim_run() is None
    assert restarted.snapshot()["runs"][0]["state"] == "interrupted"
    restarted.finish_run(running["id"])  # A stale completion cannot overwrite recovery.
    assert restarted.snapshot()["runs"][0]["state"] == "interrupted"
    restarted.retry(OWNER, engineer)
    assert restarted.claim_run()["id"] != running["id"]


def test_cancelling_parent_cancels_open_descendants(team):
    store, conductor, manager, engineer, researcher = team
    goal = store.assign(OWNER, conductor, title="Feature", acceptance="Works")
    work = store.assign(conductor, manager, title="Build", acceptance="Tests", parent_id=goal)
    leaf = store.assign(manager, engineer, title="Implement", acceptance="Tests", parent_id=work)
    store.review(OWNER, goal, "cancel", "Changed priority")
    assert {task["state"] for task in store.snapshot()["tasks"]} == {"cancelled"}
    with pytest.raises(OrganizationError):
        store.report(engineer, leaf, "done", "Too late")
