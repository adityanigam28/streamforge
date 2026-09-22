"""
StreamForge - Event-Time Temperature Aggregator

Responsibilities
---------------
- Assign telemetry events to event-time sliding windows.
- Maintain aggregation state in the RocksDB-backed StateStore.
- Support a 5-minute rolling window with configurable slide interval.
- Produce TemperatureAggregate objects.
- Restore/update state through StateStore.
- Provide deterministic behavior suitable for workers and tests.

The implementation is intentionally independent of Kafka. Kafka publishing
is handled by worker.py / kafka.py.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from .config import settings
from .models import TemperatureAggregate, TelemetryEvent
from .state import StateStore


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class AggregatorError(Exception):
    """Base exception for aggregation errors."""


class InvalidEventError(AggregatorError):
    """Raised when an invalid telemetry event is supplied."""


class InvalidWindowError(AggregatorError):
    """Raised when window configuration is invalid."""


# ---------------------------------------------------------------------------
# Window state
# ---------------------------------------------------------------------------

@dataclass
class WindowState:
    """
    Mutable state for one device/window combination.
    """

    device_id: str
    window_start: float
    window_end: float

    count: int = 0
    temperature_sum: float = 0.0
    temperature_min: float = math.inf
    temperature_max: float = -math.inf

    humidity_sum: float = 0.0
    pressure_sum: float = 0.0
    battery_sum: float = 0.0

    last_event_timestamp: float = 0.0
    last_sequence: int = -1

    def add_event(self, event: TelemetryEvent) -> None:
        """Add one telemetry event to this window."""

        timestamp = event.timestamp.timestamp()

        self.count += 1

        self.temperature_sum += float(event.temperature)
        self.temperature_min = min(
            self.temperature_min,
            float(event.temperature),
        )
        self.temperature_max = max(
            self.temperature_max,
            float(event.temperature),
        )

        self.humidity_sum += float(event.humidity)
        self.pressure_sum += float(event.pressure)
        self.battery_sum += float(event.battery)

        if timestamp >= self.last_event_timestamp:
            self.last_event_timestamp = timestamp
            self.last_sequence = int(event.sequence)

    @property
    def temperature_avg(self) -> float:
        if self.count == 0:
            return 0.0

        return self.temperature_sum / self.count

    @property
    def humidity_avg(self) -> float:
        if self.count == 0:
            return 0.0

        return self.humidity_sum / self.count

    @property
    def pressure_avg(self) -> float:
        if self.count == 0:
            return 0.0

        return self.pressure_sum / self.count

    @property
    def battery_avg(self) -> float:
        if self.count == 0:
            return 0.0

        return self.battery_sum / self.count

    def to_dict(self) -> dict:
        """Serialize window state."""

        return {
            "device_id": self.device_id,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "count": self.count,
            "temperature_sum": self.temperature_sum,
            "temperature_min": (
                None
                if self.temperature_min == math.inf
                else self.temperature_min
            ),
            "temperature_max": (
                None
                if self.temperature_max == -math.inf
                else self.temperature_max
            ),
            "humidity_sum": self.humidity_sum,
            "pressure_sum": self.pressure_sum,
            "battery_sum": self.battery_sum,
            "last_event_timestamp": self.last_event_timestamp,
            "last_sequence": self.last_sequence,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "WindowState":
        """Deserialize window state."""

        temperature_min = data.get("temperature_min")
        temperature_max = data.get("temperature_max")

        return cls(
            device_id=str(data["device_id"]),
            window_start=float(data["window_start"]),
            window_end=float(data["window_end"]),
            count=int(data.get("count", 0)),
            temperature_sum=float(data.get("temperature_sum", 0.0)),
            temperature_min=(
                math.inf
                if temperature_min is None
                else float(temperature_min)
            ),
            temperature_max=(
                -math.inf
                if temperature_max is None
                else float(temperature_max)
            ),
            humidity_sum=float(data.get("humidity_sum", 0.0)),
            pressure_sum=float(data.get("pressure_sum", 0.0)),
            battery_sum=float(data.get("battery_sum", 0.0)),
            last_event_timestamp=float(
                data.get("last_event_timestamp", 0.0)
            ),
            last_sequence=int(data.get("last_sequence", -1)),
        )


# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------

class TemperatureAggregator:
    """
    Event-time sliding-window temperature aggregator.

    Example with:
        window = 300 seconds
        slide = 60 seconds

    An event may belong to multiple windows:

        10:05:30 -> [10:01, 10:06)
                   [10:02, 10:07)
                   [10:03, 10:08)
                   [10:04, 10:09)
                   [10:05, 10:10)

    This gives the desired rolling-window behavior.
    """

    STATE_PREFIX = "window"

    def __init__(
        self,
        state: Optional[StateStore] = None,
        *,
        state_store: Optional[StateStore] = None,
        window_seconds: Optional[int] = None,
        slide_seconds: Optional[int] = None,
        allowed_lateness_seconds: Optional[int] = None,
    ) -> None:
        """
        Create an aggregator.

        Parameters
        ----------
        state:
            Optional StateStore instance.

        state_store:
            Backwards-compatible alias for state.

        window_seconds:
            Window duration. Defaults to configuration.

        slide_seconds:
            Window slide. Defaults to configuration.

        allowed_lateness_seconds:
            Allowed event-time lateness. Defaults to configuration.
        """

        if state is not None and state_store is not None:
            if state is not state_store:
                raise ValueError(
                    "Provide either 'state' or 'state_store', not two "
                    "different StateStore instances."
                )

        self.state = state or state_store or StateStore(
            name="aggregator"
        )

        # Keep state_store as an alias for compatibility with worker/tests.
        self.state_store = self.state

        self.window_seconds = int(
            window_seconds
            if window_seconds is not None
            else settings.aggregation.window_seconds
        )

        self.slide_seconds = int(
            slide_seconds
            if slide_seconds is not None
            else settings.aggregation.window_slide_seconds
        )

        self.allowed_lateness_seconds = int(
            allowed_lateness_seconds
            if allowed_lateness_seconds is not None
            else settings.aggregation.allowed_lateness_seconds
        )

        self._validate_configuration()

        self.max_event_timestamp: float = float("-inf")
        self.processed_events: int = 0
        self.processed_windows: int = 0
        self.dropped_late_events: int = 0

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def _validate_configuration(self) -> None:
        if self.window_seconds <= 0:
            raise InvalidWindowError(
                "window_seconds must be greater than zero"
            )

        if self.slide_seconds <= 0:
            raise InvalidWindowError(
                "slide_seconds must be greater than zero"
            )

        if self.slide_seconds > self.window_seconds:
            raise InvalidWindowError(
                "slide_seconds cannot be greater than window_seconds"
            )

        if self.allowed_lateness_seconds < 0:
            raise InvalidWindowError(
                "allowed_lateness_seconds cannot be negative"
            )

    # ------------------------------------------------------------------
    # Window calculations
    # ------------------------------------------------------------------

    def calculate_window_starts(
        self,
        event_timestamp: float,
    ) -> List[float]:
        """
        Return all sliding-window start timestamps containing the event.

        Windows are represented as [start, end).
        """

        slide = float(self.slide_seconds)
        window = float(self.window_seconds)

        # Normalize event time to the slide grid.
        latest_start = math.floor(event_timestamp / slide) * slide

        starts: List[float] = []

        # A window contains the event when:
        #
        # start <= event_time < start + window
        #
        # Therefore:
        #
        # event_time - window < start <= event_time
        #
        # Walking backwards over the slide grid gives every matching window.

        start = latest_start

        while start + window > event_timestamp:
            if start <= event_timestamp:
                starts.append(start)

            start -= slide

        starts.sort()

        return starts

    def get_window_bounds(
        self,
        event_timestamp: float,
    ) -> List[Tuple[float, float]]:
        """Return all window bounds containing the event."""

        return [
            (
                start,
                start + float(self.window_seconds),
            )
            for start in self.calculate_window_starts(event_timestamp)
        ]

    # ------------------------------------------------------------------
    # State keys
    # ------------------------------------------------------------------

    @staticmethod
    def _timestamp_to_string(timestamp: float) -> str:
        """
        Produce a deterministic timestamp component for a state key.
        """

        return str(int(timestamp))

    def make_state_key(
        self,
        device_id: str,
        window_start: float,
    ) -> str:
        """
        Build deterministic RocksDB key.

        Example:
            window/device-001/1756682400
        """

        return (
            f"{self.STATE_PREFIX}/"
            f"{device_id}/"
            f"{self._timestamp_to_string(window_start)}"
        )

    # Backwards-compatible alias.
    _make_state_key = make_state_key

    # ------------------------------------------------------------------
    # Event validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_event(event: TelemetryEvent) -> None:
        if not isinstance(event, TelemetryEvent):
            raise InvalidEventError(
                "event must be a TelemetryEvent instance"
            )

        if not event.device_id:
            raise InvalidEventError(
                "device_id cannot be empty"
            )

        if event.timestamp.tzinfo is None:
            raise InvalidEventError(
                "event timestamp must be timezone-aware"
            )

    # ------------------------------------------------------------------
    # Event processing
    # ------------------------------------------------------------------

    def process_event(
        self,
        event: TelemetryEvent,
    ) -> List[TemperatureAggregate]:
        """
        Process one telemetry event.

        Returns
        -------
        list[TemperatureAggregate]
            Updated aggregates for every window containing the event.
        """

        self._validate_event(event)

        event_timestamp = event.timestamp.timestamp()

        # Track maximum observed event time.
        if event_timestamp > self.max_event_timestamp:
            self.max_event_timestamp = event_timestamp

        # Drop events that are older than the allowed lateness threshold.
        if (
            self.max_event_timestamp != float("-inf")
            and event_timestamp
            < self.max_event_timestamp
            - self.allowed_lateness_seconds
        ):
            self.dropped_late_events += 1
            return []

        window_bounds = self.get_window_bounds(event_timestamp)

        aggregates: List[TemperatureAggregate] = []

        for window_start, window_end in window_bounds:
            state = self._update_window(
                event=event,
                window_start=window_start,
                window_end=window_end,
            )

            aggregate = self._state_to_aggregate(state)

            aggregates.append(aggregate)

            self.processed_windows += 1

        self.processed_events += 1

        return aggregates

    # ------------------------------------------------------------------
    # State update
    # ------------------------------------------------------------------

    def _update_window(
        self,
        event: TelemetryEvent,
        window_start: float,
        window_end: float,
    ) -> WindowState:
        """
        Update one window in StateStore.

        IMPORTANT:
        StateStore exposes `set()`, not `put()`.
        """

        key = self.make_state_key(
            event.device_id,
            window_start,
        )

        existing = self.state.get(key)

        if existing is None:
            state = WindowState(
                device_id=event.device_id,
                window_start=window_start,
                window_end=window_end,
            )
        elif isinstance(existing, WindowState):
            state = existing
        elif isinstance(existing, dict):
            state = WindowState.from_dict(existing)
        else:
            raise AggregatorError(
                f"Unexpected state type for key {key}: "
                f"{type(existing).__name__}"
            )

        state.add_event(event)

        # StateStore uses set(), not put().
        self.state.set(
            key,
            state.to_dict(),
        )

        return state

    # ------------------------------------------------------------------
    # Aggregate conversion
    # ------------------------------------------------------------------

    @staticmethod
    def _datetime_from_timestamp(
        timestamp: float,
    ) -> datetime:
        return datetime.fromtimestamp(
            timestamp,
            tz=timezone.utc,
        )

    def _state_to_aggregate(
        self,
        state: WindowState,
    ) -> TemperatureAggregate:
        """
        Convert internal WindowState to the public Pydantic model.
        """

        return TemperatureAggregate(
            device_id=state.device_id,
            window_start=self._datetime_from_timestamp(
                state.window_start
            ),
            window_end=self._datetime_from_timestamp(
                state.window_end
            ),
            count=state.count,
            avg_temperature=state.temperature_avg,
            min_temperature=(
                0.0
                if state.count == 0
                else state.temperature_min
            ),
            max_temperature=(
                0.0
                if state.count == 0
                else state.temperature_max
            ),
            avg_humidity=state.humidity_avg,
            avg_pressure=state.pressure_avg,
            avg_battery=state.battery_avg,
        )

    # ------------------------------------------------------------------
    # Direct window lookup
    # ------------------------------------------------------------------

    def get_window(
        self,
        device_id: str,
        window_start: float,
    ) -> Optional[WindowState]:
        """Load one window from StateStore."""

        key = self.make_state_key(
            device_id,
            window_start,
        )

        data = self.state.get(key)

        if data is None:
            return None

        if isinstance(data, WindowState):
            return data

        if isinstance(data, dict):
            return WindowState.from_dict(data)

        raise AggregatorError(
            f"Unexpected state type for key {key}: "
            f"{type(data).__name__}"
        )

    # ------------------------------------------------------------------
    # Window retrieval
    # ------------------------------------------------------------------

    def get_device_windows(
        self,
        device_id: str,
    ) -> List[WindowState]:
        """
        Return all persisted windows for a device.
        """

        prefix = f"{self.STATE_PREFIX}/{device_id}/"

        windows: List[WindowState] = []

        for key in self.state.keys():
            if not str(key).startswith(prefix):
                continue

            data = self.state.get(key)

            if data is None:
                continue

            if isinstance(data, WindowState):
                windows.append(data)
            elif isinstance(data, dict):
                windows.append(
                    WindowState.from_dict(data)
                )

        windows.sort(
            key=lambda item: item.window_start
        )

        return windows

    # ------------------------------------------------------------------
    # Flush
    # ------------------------------------------------------------------

    def flush(
        self,
        current_event_time: Optional[float] = None,
    ) -> List[TemperatureAggregate]:
        """
        Emit windows that are complete according to event time.

        A window is eligible when:

            window_end + allowed_lateness <= watermark

        The returned aggregates represent the final current state of those
        windows.

        The state itself is deliberately retained so that recovery and
        inspection remain possible.
        """

        if current_event_time is None:
            if self.max_event_timestamp == float("-inf"):
                return []

            watermark = self.max_event_timestamp
        else:
            watermark = float(current_event_time)

        cutoff = (
            watermark
            - self.allowed_lateness_seconds
        )

        results: List[TemperatureAggregate] = []

        for key in list(self.state.keys()):
            key_string = str(key)

            if not key_string.startswith(
                f"{self.STATE_PREFIX}/"
            ):
                continue

            data = self.state.get(key)

            if not isinstance(data, dict):
                continue

            try:
                state = WindowState.from_dict(data)
            except (KeyError, TypeError, ValueError):
                continue

            if state.window_end <= cutoff:
                results.append(
                    self._state_to_aggregate(state)
                )

        results.sort(
            key=lambda aggregate: (
                aggregate.window_start,
                aggregate.device_id,
            )
        )

        return results

    # ------------------------------------------------------------------
    # Recovery helpers
    # ------------------------------------------------------------------

    def snapshot_state(self) -> Dict[str, dict]:
        """
        Return all aggregator state as a dictionary.

        Useful for diagnostics and tests.
        """

        snapshot: Dict[str, dict] = {}

        for key in self.state.keys():
            key_string = str(key)

            if not key_string.startswith(
                f"{self.STATE_PREFIX}/"
            ):
                continue

            value = self.state.get(key)

            if isinstance(value, WindowState):
                value = value.to_dict()

            snapshot[key_string] = value

        return snapshot

    def restore_state(
        self,
        snapshot: Dict[str, dict],
    ) -> None:
        """
        Restore aggregator state from a snapshot.

        Existing keys represented in the snapshot are overwritten.
        """

        for key, value in snapshot.items():
            if not str(key).startswith(
                f"{self.STATE_PREFIX}/"
            ):
                continue

            if isinstance(value, WindowState):
                value = value.to_dict()

            self.state.set(
                str(key),
                value,
            )

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def statistics(self) -> dict:
        """Return aggregator statistics."""

        active_windows = 0

        prefix = f"{self.STATE_PREFIX}/"

        for key in self.state.keys():
            if str(key).startswith(prefix):
                active_windows += 1

        max_event_timestamp = self.max_event_timestamp

        return {
            "processed_events": self.processed_events,
            "processed_windows": self.processed_windows,
            "dropped_late_events": self.dropped_late_events,
            "active_windows": active_windows,
            "window_seconds": self.window_seconds,
            "slide_seconds": self.slide_seconds,
            "allowed_lateness_seconds": (
                self.allowed_lateness_seconds
            ),
            "max_event_timestamp": (
                None
                if max_event_timestamp == float("-inf")
                else max_event_timestamp
            ),
        }

    # Backwards-compatible aliases.
    stats = statistics
    get_stats = statistics

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """
        Close the underlying StateStore.

        This is important on Windows because RocksDB keeps a lock on the
        database directory until it is closed.
        """

        if self.state is not None and not self.state.closed:
            self.state.close()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "TemperatureAggregator":
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------

def aggregate_events(
    events: Iterable[TelemetryEvent],
    *,
    state: Optional[StateStore] = None,
    window_seconds: Optional[int] = None,
    slide_seconds: Optional[int] = None,
    allowed_lateness_seconds: Optional[int] = None,
) -> List[TemperatureAggregate]:
    """
    Aggregate a collection of telemetry events.

    Returns the latest aggregate generated for each event/window update.
    """

    aggregator = TemperatureAggregator(
        state=state,
        window_seconds=window_seconds,
        slide_seconds=slide_seconds,
        allowed_lateness_seconds=allowed_lateness_seconds,
    )

    owns_state = state is None

    try:
        results: List[TemperatureAggregate] = []

        for event in events:
            results.extend(
                aggregator.process_event(event)
            )

        return results

    finally:
        if owns_state:
            aggregator.close()


# ---------------------------------------------------------------------------
# Self-test helpers
# ---------------------------------------------------------------------------

def _make_test_event(
    *,
    device_id: str,
    timestamp: datetime,
    temperature: float,
    humidity: float = 50.0,
    pressure: float = 1013.0,
    battery: float = 90.0,
    sequence: int = 1,
) -> TelemetryEvent:
    return TelemetryEvent(
        event_id=f"{device_id}-{sequence}",
        device_id=device_id,
        timestamp=timestamp,
        temperature=temperature,
        humidity=humidity,
        pressure=pressure,
        battery=battery,
        sequence=sequence,
    )


def _self_test() -> None:
    """
    Run a lightweight local self-test.

    No Kafka is required.
    """

    print("StreamForge aggregator self-test")
    print("-" * 60)

    # --------------------------------------------------------------
    # Configuration
    # --------------------------------------------------------------

    aggregator = TemperatureAggregator(
        window_seconds=300,
        slide_seconds=60,
        allowed_lateness_seconds=0,
    )

    try:
        assert aggregator.window_seconds == 300
        assert aggregator.slide_seconds == 60

        print("Configuration: OK")

        # ----------------------------------------------------------
        # Window calculation
        # ----------------------------------------------------------

        timestamp = 300.0

        starts = aggregator.calculate_window_starts(
            timestamp
        )

        assert len(starts) == 5

        expected = [
            60.0,
            120.0,
            180.0,
            240.0,
            300.0,
        ]

        assert starts == expected

        print("Window calculation: OK")

        # ----------------------------------------------------------
        # First event
        # ----------------------------------------------------------

        event_time = datetime(
            2026,
            1,
            1,
            0,
            5,
            0,
            tzinfo=timezone.utc,
        )

        event = _make_test_event(
            device_id="device-001",
            timestamp=event_time,
            temperature=20.0,
            humidity=50.0,
            pressure=1000.0,
            battery=90.0,
            sequence=1,
        )

        aggregates = aggregator.process_event(event)

        assert len(aggregates) == 5

        print("Rolling-window assignment: OK")

        # ----------------------------------------------------------
        # StateStore compatibility
        # ----------------------------------------------------------

        windows = aggregator.get_device_windows(
            "device-001"
        )

        assert len(windows) == 5

        assert all(
            window.count == 1
            for window in windows
        )

        print("StateStore set/get compatibility: OK")

        # ----------------------------------------------------------
        # Second event
        # ----------------------------------------------------------

        event_2 = _make_test_event(
            device_id="device-001",
            timestamp=event_time,
            temperature=30.0,
            humidity=60.0,
            pressure=1020.0,
            battery=80.0,
            sequence=2,
        )

        aggregates_2 = aggregator.process_event(
            event_2
        )

        assert len(aggregates_2) == 5

        for aggregate in aggregates_2:
            assert aggregate.count == 2
            assert aggregate.avg_temperature == 25.0
            assert aggregate.min_temperature == 20.0
            assert aggregate.max_temperature == 30.0
            assert aggregate.avg_humidity == 55.0
            assert aggregate.avg_pressure == 1010.0
            assert aggregate.avg_battery == 85.0

        print("Aggregation calculation: OK")

        # ----------------------------------------------------------
        # Statistics
        # ----------------------------------------------------------

        stats = aggregator.statistics()

        assert stats["processed_events"] == 2
        assert stats["processed_windows"] == 10
        assert stats["active_windows"] == 5

        print("Statistics: OK")

        # ----------------------------------------------------------
        # Snapshot / restore
        # ----------------------------------------------------------

        snapshot = aggregator.snapshot_state()

        assert len(snapshot) == 5

        print("State snapshot: OK")

        # ----------------------------------------------------------
        # Flush
        # ----------------------------------------------------------

        flushed = aggregator.flush(
            current_event_time=event_time.timestamp()
            + 1000
        )

        assert len(flushed) == 5

        print("Window flush: OK")

    finally:
        # Critical on Windows: close RocksDB before temporary cleanup.
        aggregator.close()

    print("-" * 60)
    print("StreamForge aggregator self-test: OK")


# ---------------------------------------------------------------------------
# Module entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _self_test()