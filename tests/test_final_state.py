# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for the end-of-cell final_state hand-off."""

import json
from unittest import mock

import pytest

from managed_spark_cell_monitoring import kernelextension
from managed_spark_cell_monitoring import widget as widget_module


def make_widget():
  w = widget_module.ManagedSparkCellWidget(run_id="run-1", session_id="s")
  w.send = mock.MagicMock()
  return w


def job_start(job_id, seq):
  return {"msgtype": "sparkJobStart", "jobId": job_id, "stageIds": [job_id * 10]}, seq


def job_end(job_id, seq, status="SUCCEEDED"):
  return {"msgtype": "sparkJobEnd", "jobId": job_id, "status": status}, seq


def stage_completed(stage_id, seq):
  return {"msgtype": "sparkStageCompleted", "stageId": stage_id}, seq


def stage_active(stage_id, seq):
  return {"msgtype": "sparkStageActive", "stageId": stage_id}, seq


def final_state_messages(w):
  return [c.args[0] for c in w.send.call_args_list if c.args[0]["type"] == "final_state"]


# --- widget: retention and sending -----------------------------------------


def test_retains_only_final_events_and_sends_them_in_sequence_order():
  w = make_widget()
  for ev, seq in [
      job_start(1, 1),
      stage_active(10, 2),
      stage_active(10, 3),
      stage_completed(10, 4),
      job_end(1, 5),
      job_start(2, 6),
      stage_active(20, 7),
  ]:
    w.append_event(ev, seq)
  # A later event for the same key replaces the earlier one.
  w.append_event({"msgtype": "sparkStageCompleted", "stageId": 10, "late": True}, 8)

  w.send_final_state()

  msgs = final_state_messages(w)
  assert len(msgs) == 1
  msg = msgs[0]
  assert msg["chunk"] == 0 and msg["total_chunks"] == 1
  assert [e["sequence"] for e in msg["events"]] == [1, 5, 6, 8]
  assert [e["data"]["msgtype"] for e in msg["events"]] == [
      "sparkJobStart", "sparkJobEnd", "sparkJobStart", "sparkStageCompleted",
  ]
  assert msg["events"][-1]["data"]["late"] is True
  assert all(e["data"]["msgtype"] != "sparkStageActive" for e in msg["events"])


def test_send_final_state_is_sent_once():
  w = make_widget()
  w.append_event(*job_end(1, 1))
  w.send_final_state()
  w.send_final_state()
  assert len(final_state_messages(w)) == 1


def test_cell_without_spark_jobs_sends_nothing():
  w = make_widget()
  w.send_final_state()
  assert final_state_messages(w) == []
  assert w.final_state_sent is True


def serialized_bytes(events):
  return len(json.dumps(events, separators=(",", ":")).encode("utf-8"))


def test_chunks_are_bounded_by_serialized_size_not_count():
  w = make_widget()
  w.FINAL_STATE_CHUNK_BYTES = 4096
  # Wide SQL-style job starts: many stages with long names, ~1.3 KB each.
  for i in range(10):
    start = {
        "msgtype": "sparkJobStart",
        "jobId": i,
        "name": "collect at NativeMethodAccessorImpl.java:0",
        "stageIds": list(range(i * 10, i * 10 + 12)),
        "stageInfos": {
            str(sid): {"name": "Exchange hashpartitioning(ss_item_sk#1234, 200)" * 2,
                       "numTasks": 200}
            for sid in range(i * 10, i * 10 + 12)
        },
    }
    w.append_event(start, 2 * i + 1)
    w.append_event(*job_end(i, 2 * i + 2))

  w.send_final_state()

  msgs = final_state_messages(w)
  assert len(msgs) > 1, "expected the batch to be split"
  assert [m["chunk"] for m in msgs] == list(range(len(msgs)))
  assert all(m["total_chunks"] == len(msgs) for m in msgs)
  for m in msgs:
    assert serialized_bytes(m["events"]) <= w.FINAL_STATE_CHUNK_BYTES
  # Order and completeness are preserved across chunks.
  seqs = [e["sequence"] for m in msgs for e in m["events"]]
  assert seqs == list(range(1, 21))


def test_single_oversized_event_is_sent_alone_rather_than_dropped():
  w = make_widget()
  w.FINAL_STATE_CHUNK_BYTES = 512
  w.append_event(*job_end(1, 1))
  huge = {"msgtype": "sparkJobStart", "jobId": 2, "stageIds": [], "name": "x" * 2000}
  w.append_event(huge, 2)
  w.append_event(*job_end(2, 3))

  w.send_final_state()

  msgs = final_state_messages(w)
  assert [[e["sequence"] for e in m["events"]] for m in msgs] == [[1], [2], [3]]
  assert serialized_bytes(msgs[1]["events"]) > w.FINAL_STATE_CHUNK_BYTES


def test_event_count_guard_still_applies():
  w = make_widget()
  w.FINAL_STATE_CHUNK_EVENTS = 3
  for i in range(7):
    w.append_event(*job_end(i, i + 1))
  w.send_final_state()
  msgs = final_state_messages(w)
  assert [len(m["events"]) for m in msgs] == [3, 3, 1]


def test_chunk_by_size_helper_edge_cases():
  assert not widget_module._chunk_by_size([], 100, 10)
  events = [{"sequence": i, "data": {"k": "v" * 10}} for i in range(5)]
  one = widget_module._chunk_by_size(events, 10**6, 10**6)
  assert one == [events]
  each = widget_module._chunk_by_size(events, 1, 10**6)
  assert each == [[e] for e in events]


def test_cleanup_with_clear_history_drops_retained_finals():
  w = make_widget()
  w.append_event(*job_start(1, 1))
  w.append_event(*job_end(1, 2))
  w.append_event(*stage_completed(10, 3))
  w.cleanup(clear_history=True)
  assert not w.final_job_starts and not w.final_job_ends
  assert not w.final_stage_completions


def test_events_still_go_out_live_unchanged():
  w = make_widget()
  w.append_event(*job_start(1, 1))
  live = [c.args[0] for c in w.send.call_args_list if c.args[0]["type"] == "spark_event"]
  assert live == [{"type": "spark_event", "data": job_start(1, 1)[0], "sequence": 1}]


# --- kernel extension: when the hand-off happens ----------------------------


@pytest.fixture
def extension():
  ext = kernelextension.CellMonitorExtension(mock.MagicMock())
  ext.run_id = "run-1"
  return ext


def tracked_widget(extension, active_jobs=0, cell_finished=False):
  w = mock.MagicMock(run_id="run-1", active_jobs_count=active_jobs)
  w.cell_finished = cell_finished
  extension.active_widgets["run-1"] = w
  return w


def test_cell_end_with_no_running_jobs_sends_final_state_then_cleans_up(extension):
  w = tracked_widget(extension, active_jobs=0)
  extension.post_run_cell_hook(None)
  assert w.cell_finished is True
  w.send_final_state.assert_called_once()
  w.cleanup.assert_called_once()
  assert "run-1" not in extension.active_widgets


def test_cell_end_with_running_jobs_defers_until_the_last_job_ends(extension):
  w = tracked_widget(extension, active_jobs=2)
  extension.post_run_cell_hook(None)
  w.send_final_state.assert_not_called()  # a job may legitimately outlive the cell
  assert "run-1" in extension.active_widgets

  extension.job_to_run_id[7] = "run-1"
  extension._handle_job_end(w, {"jobId": 7})
  w.send_final_state.assert_not_called()  # one still running

  extension.job_to_run_id[8] = "run-1"
  extension._handle_job_end(w, {"jobId": 8})
  w.send_final_state.assert_called_once()
  w.cleanup.assert_called_once()
  assert "run-1" not in extension.active_widgets


def test_job_end_before_cell_end_does_not_send(extension):
  w = tracked_widget(extension, active_jobs=1, cell_finished=False)
  extension._handle_job_end(w, {"jobId": 1})
  w.send_final_state.assert_not_called()
  assert "run-1" in extension.active_widgets


def test_final_state_failure_does_not_prevent_cleanup(extension):
  w = tracked_widget(extension, active_jobs=0)
  w.send_final_state.side_effect = RuntimeError("comm closed")
  extension.post_run_cell_hook(None)
  w.cleanup.assert_called_once()
  assert "run-1" not in extension.active_widgets
