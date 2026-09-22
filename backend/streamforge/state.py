"""
StreamForge State Management

Persistent local state management for StreamForge workers.

Features:
    - RocksDB-backed persistent state
    - Thread-safe operations
    - Atomic read/update/write
    - Bulk operations
    - Checkpoint creation
    - Checkpoint recovery
    - State statistics
    - Self-test without Kafka
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable

from rocksdict import Rdict


logger = logging.getLogger(__name__)


# ============================================================
# DEFAULT PATHS
# ============================================================

DEFAULT_STATE_DIRECTORY = (
    Path("data") / "state"
)

DEFAULT_CHECKPOINT_DIRECTORY = (
    Path("data") / "checkpoints"
)


# ============================================================
# EXCEPTIONS
# ============================================================


class StateError(Exception):
    """Base exception for StreamForge state errors."""


class StateSerializationError(StateError):
    """State serialization/deserialization error."""


class StateRecoveryError(StateError):
    """State recovery error."""


# ============================================================
# SERIALIZATION
# ============================================================


def _serialize(value: Any) -> bytes:
    """Serialize a Python object to JSON bytes."""

    try:
        return json.dumps(
            value,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        ).encode("utf-8")

    except (TypeError, ValueError) as exc:
        raise StateSerializationError(
            f"Unable to serialize state value: {exc}"
        ) from exc


def _deserialize(
    value: bytes | str,
) -> Any:
    """Deserialize JSON bytes/string."""

    try:
        if isinstance(value, bytes):
            value = value.decode("utf-8")

        return json.loads(value)

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
    ) as exc:

        raise StateSerializationError(
            f"Unable to deserialize state value: {exc}"
        ) from exc


# ============================================================
# STATE STORE
# ============================================================


class StateStore:
    """
    Persistent state store backed by RocksDB.

    Example:

        store = StateStore(
            name="temperature-worker"
        )

        store.set(
            "device-001",
            {
                "temperature": 25.5,
                "count": 1
            }
        )

        value = store.get(
            "device-001"
        )

        store.close()
    """

    def __init__(
        self,
        name: str = "default",
        directory: str | os.PathLike[str] = (
            DEFAULT_STATE_DIRECTORY
        ),
        create_if_missing: bool = True,
    ) -> None:

        self.name = self._validate_name(
            name
        )

        self.base_directory = Path(
            directory
        )

        self.path = (
            self.base_directory
            / self.name
        )

        if create_if_missing:
            self.path.mkdir(
                parents=True,
                exist_ok=True,
            )

        self._lock = threading.RLock()

        self._closed = False

        self._writes = 0

        self._reads = 0

        self._deletes = 0

        self._checkpoints = 0

        logger.info(
            "Opening StreamForge state store: %s",
            self.path,
        )

        try:

            self._db = Rdict(
                str(self.path)
            )

        except Exception as exc:

            logger.exception(
                "Failed to open state store."
            )

            raise StateError(
                "Unable to open RocksDB state store "
                f"at {self.path}: {exc}"
            ) from exc

    # ========================================================
    # VALIDATION
    # ========================================================

    @staticmethod
    def _validate_name(
        name: str,
    ) -> str:

        if not isinstance(
            name,
            str,
        ):
            raise ValueError(
                "State store name must be a string."
            )

        name = name.strip()

        if not name:
            raise ValueError(
                "State store name cannot be empty."
            )

        invalid_characters = (
            "\\/:*?\"<>|"
        )

        if any(
            char in name
            for char in invalid_characters
        ):

            raise ValueError(
                f"Invalid state store name: {name!r}"
            )

        return name

    @staticmethod
    def _normalize_key(
        key: Any,
    ) -> str:

        if isinstance(
            key,
            str,
        ):
            return key

        if isinstance(
            key,
            (int, float, bool),
        ):
            return str(key)

        try:

            return json.dumps(
                key,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                default=str,
            )

        except Exception as exc:

            raise ValueError(
                f"Unable to normalize state key: {key!r}"
            ) from exc

    # ========================================================
    # INTERNAL KEY COUNT
    # ========================================================

    def _count_entries(self) -> int:
        """
        Count entries in Rdict.

        rocksdict.RdictKeys does not implement len(),
        so we explicitly iterate through the keys.
        """

        return sum(
            1
            for _ in self._db.keys()
        )

    # ========================================================
    # LIFECYCLE
    # ========================================================

    @property
    def closed(self) -> bool:

        return self._closed

    def _ensure_open(self) -> None:

        if self._closed:

            raise StateError(
                "State store is closed."
            )

    def close(self) -> None:
        """
        Flush and close RocksDB.
        """

        with self._lock:

            if self._closed:
                return

            try:

                try:

                    self._db.flush()

                except Exception as exc:

                    logger.warning(
                        "State flush during close failed: %s",
                        exc,
                    )

            finally:

                try:

                    self._db.close()

                except Exception as exc:

                    logger.warning(
                        "RocksDB close failed: %s",
                        exc,
                    )

                self._closed = True

            logger.info(
                "State store closed: %s",
                self.name,
            )

    def __enter__(
        self,
    ) -> "StateStore":

        self._ensure_open()

        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:

        self.close()

    def __del__(self) -> None:

        try:

            self.close()

        except Exception:

            pass

    # ========================================================
    # GET
    # ========================================================

    def get(
        self,
        key: Any,
        default: Any = None,
    ) -> Any:

        normalized_key = (
            self._normalize_key(key)
        )

        with self._lock:

            self._ensure_open()

            self._reads += 1

            value = self._db.get(
                normalized_key
            )

            if value is None:
                return default

            if isinstance(
                value,
                str,
            ):

                value = value.encode(
                    "utf-8"
                )

            return _deserialize(
                value
            )

    # ========================================================
    # EXISTS
    # ========================================================

    def exists(
        self,
        key: Any,
    ) -> bool:

        normalized_key = (
            self._normalize_key(key)
        )

        with self._lock:

            self._ensure_open()

            return (
                self._db.get(
                    normalized_key
                )
                is not None
            )

    # ========================================================
    # SET
    # ========================================================

    def set(
        self,
        key: Any,
        value: Any,
    ) -> None:

        normalized_key = (
            self._normalize_key(key)
        )

        serialized_value = (
            _serialize(value)
        )

        with self._lock:

            self._ensure_open()

            self._db[
                normalized_key
            ] = serialized_value

            self._writes += 1

    # ========================================================
    # DELETE
    # ========================================================

    def delete(
        self,
        key: Any,
    ) -> bool:

        normalized_key = (
            self._normalize_key(key)
        )

        with self._lock:

            self._ensure_open()

            existed = (
                self._db.get(
                    normalized_key
                )
                is not None
            )

            if existed:

                del self._db[
                    normalized_key
                ]

                self._deletes += 1

            return existed

    # ========================================================
    # CLEAR
    # ========================================================

    def clear(self) -> None:

        with self._lock:

            self._ensure_open()

            keys = list(
                self._db.keys()
            )

            for key in keys:

                del self._db[key]

            self._deletes += len(
                keys
            )

            logger.warning(
                "State store cleared: %s (%d entries)",
                self.name,
                len(keys),
            )

    # ========================================================
    # ATOMIC UPDATE
    # ========================================================

    def update(
        self,
        key: Any,
        updater: Callable[
            [Any],
            Any,
        ],
        default: Any = None,
    ) -> Any:
        """
        Atomically read, modify and write state.
        """

        if not callable(
            updater
        ):

            raise TypeError(
                "updater must be callable."
            )

        normalized_key = (
            self._normalize_key(key)
        )

        with self._lock:

            self._ensure_open()

            existing = (
                self._db.get(
                    normalized_key
                )
            )

            if existing is None:

                current = default

            else:

                if isinstance(
                    existing,
                    str,
                ):

                    existing = (
                        existing.encode(
                            "utf-8"
                        )
                    )

                current = _deserialize(
                    existing
                )

            updated = updater(
                current
            )

            self._db[
                normalized_key
            ] = _serialize(
                updated
            )

            self._writes += 1

            return updated

    # ========================================================
    # BULK GET
    # ========================================================

    def get_many(
        self,
        keys: Iterable[Any],
    ) -> Dict[str, Any]:

        result: Dict[
            str,
            Any,
        ] = {}

        with self._lock:

            self._ensure_open()

            for key in keys:

                normalized_key = (
                    self._normalize_key(
                        key
                    )
                )

                value = (
                    self._db.get(
                        normalized_key
                    )
                )

                self._reads += 1

                if value is None:
                    continue

                if isinstance(
                    value,
                    str,
                ):

                    value = value.encode(
                        "utf-8"
                    )

                result[
                    normalized_key
                ] = _deserialize(
                    value
                )

        return result

    # ========================================================
    # BULK SET
    # ========================================================

    def set_many(
        self,
        values: Dict[Any, Any],
    ) -> None:

        with self._lock:

            self._ensure_open()

            for key, value in values.items():

                normalized_key = (
                    self._normalize_key(
                        key
                    )
                )

                self._db[
                    normalized_key
                ] = _serialize(
                    value
                )

                self._writes += 1

    # ========================================================
    # KEYS
    # ========================================================

    def keys(self) -> list[str]:

        with self._lock:

            self._ensure_open()

            return [
                str(key)
                for key in self._db.keys()
            ]

    # ========================================================
    # ITEMS
    # ========================================================

    def items(
        self,
    ) -> Dict[str, Any]:

        with self._lock:

            self._ensure_open()

            result: Dict[
                str,
                Any,
            ] = {}

            for key in self._db.keys():

                value = (
                    self._db.get(
                        key
                    )
                )

                if value is None:
                    continue

                if isinstance(
                    value,
                    str,
                ):

                    value = value.encode(
                        "utf-8"
                    )

                result[
                    str(key)
                ] = _deserialize(
                    value
                )

            return result

    # ========================================================
    # LENGTH
    # ========================================================

    def __len__(
        self,
    ) -> int:
        """
        Return the number of state entries.

        rocksdict.Rdict and RdictKeys do not
        implement Python len(), therefore entries
        are counted explicitly.
        """

        with self._lock:

            self._ensure_open()

            return self._count_entries()

    # ========================================================
    # FLUSH
    # ========================================================

    def flush(self) -> None:

        with self._lock:

            self._ensure_open()

            try:

                self._db.flush()

            except Exception as exc:

                raise StateError(
                    f"Failed to flush state store: {exc}"
                ) from exc

    # ========================================================
    # CHECKPOINT
    # ========================================================

    def checkpoint(
        self,
        checkpoint_directory:
            str | os.PathLike[str]
            = DEFAULT_CHECKPOINT_DIRECTORY,
    ) -> Path:
        """
        Create a logical JSON checkpoint.
        """

        checkpoint_root = Path(
            checkpoint_directory
        )

        checkpoint_root.mkdir(
            parents=True,
            exist_ok=True,
        )

        timestamp = int(
            time.time() * 1000
        )

        checkpoint_path = (
            checkpoint_root
            / f"{self.name}-{timestamp}"
        )

        checkpoint_path.mkdir(
            parents=True,
            exist_ok=False,
        )

        with self._lock:

            self._ensure_open()

            self.flush()

            state = self.items()

            metadata = {
                "application": "StreamForge",
                "state_store": self.name,
                "created_at": time.time(),
                "created_at_epoch_ms": timestamp,
                "entry_count": len(state),
                "format_version": 1,
            }

            try:

                with open(
                    checkpoint_path
                    / "checkpoint.json",
                    "w",
                    encoding="utf-8",
                ) as file:

                    json.dump(
                        metadata,
                        file,
                        indent=2,
                        ensure_ascii=False,
                    )

                with open(
                    checkpoint_path
                    / "state.json",
                    "w",
                    encoding="utf-8",
                ) as file:

                    json.dump(
                        state,
                        file,
                        indent=2,
                        ensure_ascii=False,
                    )

            except (
                OSError,
                TypeError,
                ValueError,
            ) as exc:

                shutil.rmtree(
                    checkpoint_path,
                    ignore_errors=True,
                )

                raise StateError(
                    f"Failed to create checkpoint: {exc}"
                ) from exc

            self._checkpoints += 1

        logger.info(
            "Created state checkpoint: %s",
            checkpoint_path,
        )

        return checkpoint_path

    # ========================================================
    # RESTORE CHECKPOINT
    # ========================================================

    def restore_checkpoint(
        self,
        checkpoint_path:
            str | os.PathLike[str],
        clear_existing: bool = True,
    ) -> int:
        """
        Restore state from a checkpoint.
        """

        checkpoint_path = Path(
            checkpoint_path
        )

        state_file = (
            checkpoint_path
            / "state.json"
        )

        if not state_file.exists():

            raise StateRecoveryError(
                "Checkpoint state file does not exist: "
                f"{state_file}"
            )

        try:

            with open(
                state_file,
                "r",
                encoding="utf-8",
            ) as file:

                state = json.load(
                    file
                )

        except (
            OSError,
            json.JSONDecodeError,
        ) as exc:

            raise StateRecoveryError(
                f"Unable to read checkpoint: {exc}"
            ) from exc

        if not isinstance(
            state,
            dict,
        ):

            raise StateRecoveryError(
                "Checkpoint state must contain "
                "a JSON object."
            )

        with self._lock:

            self._ensure_open()

            if clear_existing:
                self.clear()

            for key, value in state.items():

                normalized_key = (
                    self._normalize_key(
                        key
                    )
                )

                self._db[
                    normalized_key
                ] = _serialize(
                    value
                )

                self._writes += 1

            self.flush()

        logger.info(
            "Restored %d state entries from checkpoint: %s",
            len(state),
            checkpoint_path,
        )

        return len(state)

    # ========================================================
    # STATISTICS
    # ========================================================

    def stats(
        self,
    ) -> Dict[str, Any]:

        with self._lock:

            self._ensure_open()

            return {
                "name": self.name,
                "path": str(self.path),
                "entries": self._count_entries(),
                "reads": self._reads,
                "writes": self._writes,
                "deletes": self._deletes,
                "checkpoints": self._checkpoints,
                "closed": self._closed,
            }


# ============================================================
# STATE MANAGER
# ============================================================


class StateManager:
    """
    Manages multiple named StateStore instances.
    """

    def __init__(
        self,
        directory:
            str | os.PathLike[str]
            = DEFAULT_STATE_DIRECTORY,
    ) -> None:

        self.directory = Path(
            directory
        )

        self.directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._stores: Dict[
            str,
            StateStore,
        ] = {}

        self._lock = threading.RLock()

    def get_store(
        self,
        name: str,
    ) -> StateStore:

        with self._lock:

            if name not in self._stores:

                self._stores[name] = (
                    StateStore(
                        name=name,
                        directory=self.directory,
                    )
                )

            return self._stores[name]

    def close_store(
        self,
        name: str,
    ) -> None:

        with self._lock:

            store = self._stores.pop(
                name,
                None,
            )

            if store is not None:
                store.close()

    def close_all(
        self,
    ) -> None:

        with self._lock:

            stores = list(
                self._stores.values()
            )

            self._stores.clear()

            for store in stores:

                try:

                    store.close()

                except Exception:

                    logger.exception(
                        "Error closing state store: %s",
                        store.name,
                    )

    def stats(
        self,
    ) -> Dict[
        str,
        Dict[str, Any],
    ]:

        with self._lock:

            return {
                name: store.stats()
                for name, store
                in self._stores.items()
            }

    def __enter__(
        self,
    ) -> "StateManager":

        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:

        self.close_all()


# ============================================================
# SELF TEST
# ============================================================


def run_self_test() -> None:
    """
    Complete StateStore self-test.

    No Kafka is required.
    """

    print(
        "StreamForge state store self-test"
    )

    print(
        "-" * 60
    )

    temporary_directory = tempfile.mkdtemp(
        prefix="streamforge-state-test-"
    )

    store: StateStore | None = None

    recovered_store: StateStore | None = None

    try:

        # ----------------------------------------------------
        # CREATE
        # ----------------------------------------------------

        store = StateStore(
            name="self-test",
            directory=temporary_directory,
        )

        # ----------------------------------------------------
        # SET / GET
        # ----------------------------------------------------

        store.set(
            "device-001",
            {
                "temperature": 25.5,
                "count": 1,
            },
        )

        assert store.get(
            "device-001"
        ) == {
            "temperature": 25.5,
            "count": 1,
        }

        print(
            "Set/get: OK"
        )

        # ----------------------------------------------------
        # DEFAULT
        # ----------------------------------------------------

        assert store.get(
            "missing",
            default="default-value",
        ) == "default-value"

        print(
            "Default value: OK"
        )

        # ----------------------------------------------------
        # EXISTS
        # ----------------------------------------------------

        assert store.exists(
            "device-001"
        )

        assert not store.exists(
            "missing"
        )

        print(
            "Exists: OK"
        )

        # ----------------------------------------------------
        # ATOMIC UPDATE
        # ----------------------------------------------------

        updated = store.update(
            "device-001",
            lambda state: {
                **state,
                "count": (
                    state["count"] + 1
                ),
            },
        )

        assert updated[
            "count"
        ] == 2

        assert store.get(
            "device-001"
        )["count"] == 2

        print(
            "Atomic update: OK"
        )

        # ----------------------------------------------------
        # BULK OPERATIONS
        # ----------------------------------------------------

        store.set_many(
            {
                "device-002": {
                    "temperature": 30.0,
                },
                "device-003": {
                    "temperature": 31.0,
                },
            }
        )

        values = store.get_many(
            [
                "device-002",
                "device-003",
            ]
        )

        assert len(values) == 2

        print(
            "Bulk operations: OK"
        )

        # ----------------------------------------------------
        # LENGTH
        # ----------------------------------------------------

        assert len(store) == 3

        print(
            "Length: OK"
        )

        # ----------------------------------------------------
        # CHECKPOINT
        # ----------------------------------------------------

        checkpoint = store.checkpoint(
            Path(
                temporary_directory
            )
            / "checkpoints"
        )

        assert (
            checkpoint
            / "state.json"
        ).exists()

        assert (
            checkpoint
            / "checkpoint.json"
        ).exists()

        print(
            "Checkpoint: OK"
        )

        # ----------------------------------------------------
        # CLOSE ORIGINAL
        # ----------------------------------------------------

        store.close()

        store = None

        # ----------------------------------------------------
        # RECOVERY
        # ----------------------------------------------------

        recovered_store = StateStore(
            name="recovered",
            directory=temporary_directory,
        )

        count = (
            recovered_store.restore_checkpoint(
                checkpoint
            )
        )

        assert count == 3

        assert len(
            recovered_store
        ) == 3

        assert recovered_store.get(
            "device-001"
        )["count"] == 2

        print(
            "Recovery: OK"
        )

        # ----------------------------------------------------
        # STATISTICS
        # ----------------------------------------------------

        stats = (
            recovered_store.stats()
        )

        assert (
            stats["entries"] == 3
        )

        print(
            "Statistics: OK"
        )

        # ----------------------------------------------------
        # CLOSE RECOVERED STORE
        # ----------------------------------------------------

        recovered_store.close()

        recovered_store = None

        print(
            "-" * 60
        )

        print(
            "StreamForge state store self-test: OK"
        )

    finally:

        # ----------------------------------------------------
        # WINDOWS-SAFE CLEANUP
        # ----------------------------------------------------

        if store is not None:

            try:

                store.close()

            except Exception:

                logger.exception(
                    "Error closing test state store."
                )

        if recovered_store is not None:

            try:

                recovered_store.close()

            except Exception:

                logger.exception(
                    "Error closing recovered state store."
                )

        # Give RocksDB time to release file locks.
        time.sleep(
            0.2
        )

        shutil.rmtree(
            temporary_directory,
            ignore_errors=True,
        )


# ============================================================
# MAIN
# ============================================================


def main() -> None:

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    run_self_test()


# ============================================================
# ENTRY POINT
# ============================================================


if __name__ == "__main__":

    main()