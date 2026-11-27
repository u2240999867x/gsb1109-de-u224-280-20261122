# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import unittest

from botocore.devicearchive import (
    ArchiveWindow,
    BootSegment,
    ClockAdjustment,
    CONFLICT,
    DeviceArchiveCollector,
    MillisecondRecord,
    MillisecondRequest,
    OK,
    RecordingSource,
    WholeSecondRecord,
    WholeSecondRequest,
    adjudicate,
    build_collection_plan,
    verify_coverage,
)

MS = 1000
SECOND = 1_000_000


def worked_segment():
    return BootSegment(
        boot_id="boot-1",
        initial_offset=0,
        adjustments=[ClockAdjustment(400 * MS, 600 * MS, -1000 * MS)],
    )


def worked_window():
    return ArchiveWindow(450 * MS, 550 * MS)


class MillisecondFake:
    """Millisecond JSON service: exact device times, half-open filtering."""

    def __init__(self, rows):
        self.rows = rows

    def __call__(self, request):
        for sequence, device_millis in self.rows:
            device_micros = device_millis * MS
            if request.device_start <= device_micros < request.device_end:
                yield MillisecondRecord(sequence, device_millis)


class WholeSecondFake:
    """Whole-second header service: bucket labels, half-open filtering."""

    def __init__(self, rows):
        self.rows = rows

    def __call__(self, request):
        for sequence, bucket_start_millis in self.rows:
            bucket_micros = bucket_start_millis * MS
            if request.device_start <= bucket_micros < request.device_end:
                yield WholeSecondRecord(sequence, bucket_start_millis)


WORKED_SECOND_ROWS = [
    (39, 0),
    (40, 0),
    (41, -1000),
    (42, 0),
]


class CollectionPlanTest(unittest.TestCase):
    def test_plan_matches_spec_ranges(self):
        plan = build_collection_plan(worked_segment(), worked_window())

        self.assertEqual(
            plan.millisecond,
            [(-550 * MS, -450 * MS), (450 * MS, 550 * MS)],
        )
        self.assertEqual(plan.whole_second, [(-1000 * MS, 1000 * MS)])

    def test_distinct_ranges_are_not_merged_at_millisecond_granularity(self):
        plan = build_collection_plan(worked_segment(), worked_window())

        self.assertEqual(len(plan.millisecond), 2)
        total_span = sum(end - start for start, end in plan.millisecond)
        self.assertEqual(total_span, 200 * MS)

    def test_whole_second_snaps_outward_to_second_boundaries(self):
        segment = BootSegment("b", 0, [])
        window = ArchiveWindow(450 * MS, 550 * MS)

        plan = build_collection_plan(segment, window)

        self.assertEqual(plan.whole_second, [(0, 1000 * MS)])

    def test_millisecond_snaps_outward_to_millisecond_boundaries(self):
        segment = BootSegment("b", 10, [])
        window = ArchiveWindow(450 * MS + 7, 550 * MS + 3)

        plan = build_collection_plan(segment, window)

        self.assertEqual(
            plan.millisecond,
            [(450 * MS, 551 * MS)],
        )

    def test_window_must_be_non_empty(self):
        with self.assertRaises(ValueError):
            ArchiveWindow(100, 100)


class RequestSerializationTest(unittest.TestCase):
    def test_millisecond_body_uses_unix_millis(self):
        request = MillisecondRequest(-550 * MS, -450 * MS)

        self.assertEqual(request.start_millis, -550)
        self.assertEqual(request.end_millis, -450)
        self.assertEqual(
            request.body,
            '{"start_time_millis":-550,"end_time_millis":-450}',
        )

    def test_whole_second_headers_are_rfc822_gmt(self):
        request = WholeSecondRequest(-1000 * MS, 1000 * MS)

        self.assertEqual(request.start_seconds, -1)
        self.assertEqual(request.end_seconds, 1)
        self.assertEqual(
            request.headers["X-Device-Start-Date"],
            "Wed, 31 Dec 1969 23:59:59 GMT",
        )
        self.assertEqual(
            request.headers["X-Device-End-Date"],
            "Thu, 01 Jan 1970 00:00:01 GMT",
        )

    def test_rfc822_header_round_trips_negative_seconds(self):
        request = WholeSecondRequest(-1000 * MS, 1000 * MS)

        self.assertEqual(
            WholeSecondRequest.parse_header(
                request.headers["X-Device-Start-Date"]
            ),
            -1,
        )
        self.assertEqual(
            WholeSecondRequest.parse_header(
                request.headers["X-Device-End-Date"]
            ),
            1,
        )


class WorkedScenarioTest(unittest.TestCase):
    def _collect(self, millisecond_rows):
        millisecond_source = RecordingSource(MillisecondFake(millisecond_rows))
        whole_second_source = RecordingSource(
            WholeSecondFake(WORKED_SECOND_ROWS)
        )
        collector = DeviceArchiveCollector(
            millisecond_source, whole_second_source
        )
        decision = collector.collect(worked_segment(), worked_window())
        return (
            decision,
            millisecond_source,
            whole_second_source,
        )

    def test_requests_match_spec_coverage(self):
        decision, millisecond_source, whole_second_source = self._collect(
            [(40, 520), (41, -460)]
        )

        self.assertEqual(
            [
                (request.start_millis, request.end_millis)
                for request in millisecond_source.requests
            ],
            [(-550, -450), (450, 550)],
        )
        self.assertEqual(
            [
                (request.start_seconds, request.end_seconds)
                for request in whole_second_source.requests
            ],
            [(-1, 1)],
        )
        missing, extra = verify_coverage(
            decision.plan.millisecond, millisecond_source.requests
        )
        self.assertEqual((missing, extra), ([], []))
        missing, extra = verify_coverage(
            decision.plan.whole_second, whole_second_source.requests
        )
        self.assertEqual((missing, extra), ([], []))

    def test_does_not_fetch_unrelated_middle_device_time(self):
        _, millisecond_source, _ = self._collect([(40, 520), (41, -460)])

        for request in millisecond_source.requests:
            self.assertFalse(
                request.device_start <= -450 * MS < request.device_end
            )
            self.assertFalse(
                request.device_start <= 449 * MS < request.device_end
            )

    def test_adjudication_include_exclude_pending(self):
        decision, _, _ = self._collect([(40, 520), (41, -460)])

        self.assertEqual(decision.status, OK)
        self.assertEqual(decision.included, [40, 41])
        self.assertEqual(decision.excluded, [42])
        self.assertEqual(decision.pending, [39])

    def test_raw_evidence_is_retained_per_sequence(self):
        decision, _, _ = self._collect([(40, 520), (41, -460)])

        self.assertIsNone(decision.evidence[39].millisecond_millis)
        self.assertEqual(decision.evidence[39].whole_second_millis, 0)
        self.assertEqual(decision.evidence[40].millisecond_millis, 520)
        self.assertEqual(decision.evidence[40].whole_second_millis, 0)
        self.assertEqual(decision.evidence[41].millisecond_millis, -460)
        self.assertEqual(decision.evidence[41].whole_second_millis, -1000)
        self.assertIsNone(decision.evidence[42].millisecond_millis)
        self.assertEqual(decision.evidence[42].whole_second_millis, 0)

    def test_changed_evidence_on_recollection_reports_conflict(self):
        millisecond_rows = [(40, 520), (41, -460)]
        millisecond_source = RecordingSource(MillisecondFake(millisecond_rows))
        whole_second_source = RecordingSource(
            WholeSecondFake(WORKED_SECOND_ROWS)
        )
        collector = DeviceArchiveCollector(
            millisecond_source, whole_second_source
        )

        first = collector.collect(worked_segment(), worked_window())
        self.assertEqual(first.included, [40, 41])

        millisecond_rows[:] = [(40, 520), (41, -490)]
        second = collector.collect(worked_segment(), worked_window())

        self.assertEqual(second.status, CONFLICT)
        self.assertEqual(second.included, [])
        self.assertEqual(second.excluded, [])
        self.assertEqual(second.pending, [])
        self.assertEqual(second.feasible_timeline_count, 0)
        self.assertEqual(
            sorted(second.evidence), [39, 40, 41, 42]
        )


class BucketSemanticsTest(unittest.TestCase):
    def test_bucket_straddling_window_boundary_is_pending(self):
        segment = BootSegment("b", 0, [])
        window = ArchiveWindow(500 * MS, 1500 * MS)
        decision = adjudicate(
            segment,
            window,
            millisecond_records=[MillisecondRecord(2, 1200)],
            whole_second_records=[WholeSecondRecord(1, 0)],
        )

        self.assertEqual(decision.included, [2])
        self.assertEqual(decision.pending, [1])
        self.assertEqual(decision.excluded, [])

    def test_half_open_boundaries(self):
        segment = BootSegment("b", 0, [])
        window = ArchiveWindow(1000 * MS, 2000 * MS)
        decision = adjudicate(
            segment,
            window,
            millisecond_records=[
                MillisecondRecord(1, 1000),
                MillisecondRecord(2, 2000),
            ],
            whole_second_records=[],
        )

        self.assertEqual(decision.included, [1])
        self.assertEqual(decision.excluded, [2])

    def test_whole_second_label_covers_full_second(self):
        segment = BootSegment("b", 0, [])
        window = ArchiveWindow(0, 500 * MS)
        decision = adjudicate(
            segment,
            window,
            millisecond_records=[],
            whole_second_records=[WholeSecondRecord(1, 0)],
        )

        self.assertEqual(decision.pending, [1])


class AdjustmentUncertaintyTest(unittest.TestCase):
    def _segment(self, utc_start, utc_end):
        return BootSegment(
            "b",
            0,
            [ClockAdjustment(utc_start, utc_end, -5000 * MS)],
        )

    def test_wide_candidate_interval_leaves_event_pending(self):
        segment = self._segment(0, 4000 * MS)
        window = ArchiveWindow(1000 * MS, 2000 * MS)

        decision = adjudicate(
            segment,
            window,
            [MillisecondRecord(1, 1500)],
            [],
        )

        self.assertEqual(decision.pending, [1])

    def test_narrow_candidate_interval_pins_event_inside(self):
        segment = self._segment(8000 * MS, 9000 * MS)
        window = ArchiveWindow(1000 * MS, 2000 * MS)

        decision = adjudicate(
            segment,
            window,
            [MillisecondRecord(1, 1500)],
            [],
        )

        self.assertEqual(decision.included, [1])

    def test_multiple_adjustments_keep_ordered_step_times(self):
        segment = BootSegment(
            "b",
            0,
            [
                ClockAdjustment(0, 1000 * MS, 1000 * MS),
                ClockAdjustment(2000 * MS, 3000 * MS, 1000 * MS),
            ],
        )
        self.assertEqual(segment.offsets(), [0, 1000 * MS, 2000 * MS])

        window = ArchiveWindow(2500 * MS, 3500 * MS)
        plan = build_collection_plan(segment, window)

        self.assertEqual(
            plan.millisecond,
            [
                (3500 * MS, 4000 * MS),
                (4500 * MS, 5500 * MS),
            ],
        )


class ConstantOffsetTest(unittest.TestCase):
    def test_initial_offset_shifts_device_ranges(self):
        segment = BootSegment("b", 250 * MS, [])
        window = ArchiveWindow(0, 1000 * MS)

        plan = build_collection_plan(segment, window)

        self.assertEqual(plan.millisecond, [(250 * MS, 1250 * MS)])

    def test_adjunction_uses_offset_to_recover_utc(self):
        segment = BootSegment("b", 250 * MS, [])
        window = ArchiveWindow(0, 1000 * MS)

        inside = adjudicate(
            segment, window, [MillisecondRecord(1, 750)], []
        )
        outside = adjudicate(
            segment, window, [MillisecondRecord(2, 1300)], []
        )

        self.assertEqual(inside.included, [1])
        self.assertEqual(outside.excluded, [2])


class CoverageVerificationTest(unittest.TestCase):
    def setUp(self):
        self.plan = [(-550 * MS, -450 * MS), (450 * MS, 550 * MS)]

    def test_exact_coverage(self):
        requests = [
            MillisecondRequest(-550 * MS, -450 * MS),
            MillisecondRequest(450 * MS, 550 * MS),
        ]

        missing, extra = verify_coverage(self.plan, requests)

        self.assertEqual((missing, extra), ([], []))

    def test_detects_missing_range(self):
        requests = [MillisecondRequest(450 * MS, 550 * MS)]

        missing, extra = verify_coverage(self.plan, requests)

        self.assertEqual(missing, [(-550 * MS, -450 * MS)])
        self.assertEqual(extra, [])

    def test_detects_over_fetch(self):
        requests = [
            MillisecondRequest(-550 * MS, -450 * MS),
            MillisecondRequest(450 * MS, 650 * MS),
        ]

        missing, extra = verify_coverage(self.plan, requests)

        self.assertEqual(missing, [])
        self.assertEqual(extra, [(550 * MS, 650 * MS)])

    def test_detects_gap_inside_a_single_request_range(self):
        requests = [
            MillisecondRequest(-550 * MS, -500 * MS),
            MillisecondRequest(-500 * MS, -450 * MS),
            MillisecondRequest(450 * MS, 550 * MS),
        ]

        missing, extra = verify_coverage(self.plan, requests)

        self.assertEqual((missing, extra), ([], []))


class DuplicateDeviceTimeTest(unittest.TestCase):
    def test_same_device_time_in_both_pieces_returns_both_events(self):
        segment = worked_segment()
        window = worked_window()
        rows = [(10, 500), (11, 500)]
        source = RecordingSource(MillisecondFake(rows))
        collector = DeviceArchiveCollector(source)

        decision = collector.collect(segment, window)

        returned_sequences = {
            record.sequence
            for request in source.requests
            for record in MillisecondFake(rows)(request)
        }
        self.assertEqual(returned_sequences, {10, 11})
        self.assertEqual(
            sorted(decision.evidence), [10, 11]
        )


if __name__ == "__main__":
    unittest.main()
