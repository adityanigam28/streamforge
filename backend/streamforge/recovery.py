"""
StreamForge State Recovery Manager

Handles:

1. Creating checkpoints
2. Listing checkpoints
3. Validating checkpoints
4. Restoring checkpoints
5. Checkpoint retention
6. Recovery statistics
7. Recovery self-testing

This module is designed to work with the actual
StateStore API implemented in streamforge.state.
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
import time
import uuid

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from .state import StateStore


logger = logging.getLogger(__name__)


DEFAULT_CHECKPOINT_RETENTION = 5


# ============================================================
# EXCEPTIONS
# ============================================================


class RecoveryError(Exception):
    """Base exception for StreamForge recovery errors."""


class CheckpointNotFoundError(RecoveryError):
    """Raised when a requested checkpoint does not exist."""


class InvalidCheckpointError(RecoveryError):
    """Raised when a checkpoint is invalid or incomplete."""


# ============================================================
# DATA CLASSES
# ============================================================


@dataclass
class RecoveryMetadata:
    """Metadata describing a StreamForge checkpoint."""

    checkpoint_id: str
    checkpoint_path: str
    created_at: float
    state_entries: int
    valid: bool
    source_store: str
    version: str = "1.0"

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to a dictionary."""

        return asdict(self)


@dataclass
class RecoveryStatistics:
    """Runtime recovery statistics."""

    checkpoints_created: int = 0
    checkpoints_validated: int = 0
    checkpoints_restored: int = 0
    recovery_failures: int = 0

    entries_restored: int = 0

    last_checkpoint_id: Optional[str] = None
    last_recovery_time: Optional[float] = None
    last_recovery_duration: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert statistics to a dictionary."""

        return asdict(self)


# ============================================================
# RECOVERY MANAGER
# ============================================================


class RecoveryManager:
    """
    High-level checkpoint and recovery manager.

    The StateStore owns the actual state database.

    RecoveryManager provides:

        StateStore
             |
             +---- checkpoint()
             |
             v
        RecoveryManager
             |
             +---- metadata
             +---- validation
             +---- retention
             +---- restore
             +---- statistics

    IMPORTANT:

    StateStore.restore_checkpoint() is an INSTANCE method:

        store.restore_checkpoint(
            checkpoint_path,
            clear_existing=True
        )

    It does not create a new StateStore.
    """

    def __init__(
        self,
        state_store: StateStore,
        checkpoint_directory: Optional[
            str | Path
        ] = None,
        retention: int = DEFAULT_CHECKPOINT_RETENTION,
    ) -> None:

        if state_store is None:

            raise ValueError(
                "state_store cannot be None."
            )

        if retention <= 0:

            raise ValueError(
                "retention must be greater than zero."
            )

        self.state_store = state_store

        if checkpoint_directory is None:

            checkpoint_directory = (
                Path("data")
                / "checkpoints"
            )

        self.checkpoint_directory = Path(
            checkpoint_directory
        )

        self.checkpoint_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.retention = int(
            retention
        )

        self._statistics = (
            RecoveryStatistics()
        )

        logger.info(
            "RecoveryManager initialized: "
            "checkpoint_directory=%s, retention=%d",
            self.checkpoint_directory,
            self.retention,
        )

    # ========================================================
    # INTERNAL HELPERS
    # ========================================================

    def _store_name(self) -> str:

        name = getattr(
            self.state_store,
            "name",
            None,
        )

        if name:

            return str(name)

        return "unknown"

    def _checkpoint_path(
        self,
        checkpoint_id: str,
    ) -> Path:

        return (
            self.checkpoint_directory
            / checkpoint_id
        )

    @staticmethod
    def _metadata_path(
        checkpoint_path: Path,
    ) -> Path:

        return (
            checkpoint_path
            / "streamforge-recovery.json"
        )

    # ========================================================
    # CREATE CHECKPOINT
    # ========================================================

    def create_checkpoint(
        self,
        checkpoint_id: Optional[str] = None,
    ) -> RecoveryMetadata:
        """
        Create a StreamForge checkpoint.

        StateStore.checkpoint() creates the actual checkpoint.

        RecoveryManager then copies that checkpoint into its
        managed checkpoint directory.
        """

        if checkpoint_id is None:

            checkpoint_id = (
                "checkpoint-"
                f"{int(time.time() * 1000)}-"
                f"{uuid.uuid4().hex[:8]}"
            )

        checkpoint_id = str(
            checkpoint_id
        )

        destination = (
            self._checkpoint_path(
                checkpoint_id
            )
        )

        if destination.exists():

            raise RecoveryError(
                "Checkpoint already exists: "
                f"{checkpoint_id}"
            )

        logger.info(
            "Creating StreamForge checkpoint: %s",
            checkpoint_id,
        )

        try:

            # ------------------------------------------------
            # Use the real StateStore API.
            # ------------------------------------------------

            created_path = (
                self.state_store.checkpoint()
            )

            created_path = Path(
                created_path
            )

            if not created_path.exists():

                raise RecoveryError(
                    "StateStore.checkpoint() returned "
                    "a path that does not exist: "
                    f"{created_path}"
                )

            if not created_path.is_dir():

                raise RecoveryError(
                    "StateStore checkpoint is not "
                    "a directory: "
                    f"{created_path}"
                )

            destination.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            shutil.copytree(
                created_path,
                destination,
            )

            # ------------------------------------------------
            # Count state entries.
            #
            # Use list(keys()) because the underlying
            # RocksDB keys object does not support len().
            # ------------------------------------------------

            entry_count = len(
                list(
                    self.state_store.keys()
                )
            )

            metadata = RecoveryMetadata(
                checkpoint_id=checkpoint_id,
                checkpoint_path=str(
                    destination
                ),
                created_at=time.time(),
                state_entries=entry_count,
                valid=True,
                source_store=self._store_name(),
            )

            self._write_metadata(
                destination,
                metadata,
            )

            self._statistics.checkpoints_created += 1

            self._statistics.last_checkpoint_id = (
                checkpoint_id
            )

            logger.info(
                "Checkpoint created successfully: %s",
                destination,
            )

            # ------------------------------------------------
            # Apply retention policy.
            # ------------------------------------------------

            self.cleanup_old_checkpoints()

            return metadata

        except RecoveryError:

            self._statistics.recovery_failures += 1

            raise

        except Exception as exc:

            self._statistics.recovery_failures += 1

            logger.exception(
                "Failed to create checkpoint '%s'.",
                checkpoint_id,
            )

            raise RecoveryError(
                f"Failed to create checkpoint "
                f"'{checkpoint_id}': {exc}"
            ) from exc

    # ========================================================
    # WRITE METADATA
    # ========================================================

    def _write_metadata(
        self,
        checkpoint_path: Path,
        metadata: RecoveryMetadata,
    ) -> None:

        checkpoint_path.mkdir(
            parents=True,
            exist_ok=True,
        )

        metadata_path = (
            self._metadata_path(
                checkpoint_path
            )
        )

        metadata_path.write_text(
            json.dumps(
                metadata.to_dict(),
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    # ========================================================
    # READ METADATA
    # ========================================================

    def _read_metadata(
        self,
        checkpoint_path: Path,
    ) -> RecoveryMetadata:

        metadata_path = (
            self._metadata_path(
                checkpoint_path
            )
        )

        if not metadata_path.exists():

            raise InvalidCheckpointError(
                "Checkpoint metadata is missing: "
                f"{metadata_path}"
            )

        try:

            data = json.loads(
                metadata_path.read_text(
                    encoding="utf-8"
                )
            )

            return RecoveryMetadata(
                checkpoint_id=str(
                    data["checkpoint_id"]
                ),
                checkpoint_path=str(
                    data["checkpoint_path"]
                ),
                created_at=float(
                    data["created_at"]
                ),
                state_entries=int(
                    data["state_entries"]
                ),
                valid=bool(
                    data["valid"]
                ),
                source_store=str(
                    data["source_store"]
                ),
                version=str(
                    data.get(
                        "version",
                        "1.0",
                    )
                ),
            )

        except (
            OSError,
            ValueError,
            KeyError,
            TypeError,
            json.JSONDecodeError,
        ) as exc:

            raise InvalidCheckpointError(
                "Invalid checkpoint metadata: "
                f"{metadata_path}"
            ) from exc

    # ========================================================
    # LIST CHECKPOINTS
    # ========================================================

    def list_checkpoints(
        self,
    ) -> List[RecoveryMetadata]:
        """Return all valid managed checkpoints."""

        checkpoints: List[
            RecoveryMetadata
        ] = []

        if not self.checkpoint_directory.exists():

            return checkpoints

        for path in (
            self.checkpoint_directory.iterdir()
        ):

            if not path.is_dir():

                continue

            try:

                metadata = (
                    self._read_metadata(
                        path
                    )
                )

                checkpoints.append(
                    metadata
                )

            except InvalidCheckpointError:

                logger.warning(
                    "Ignoring invalid checkpoint: %s",
                    path,
                )

        checkpoints.sort(
            key=lambda item: item.created_at,
            reverse=True,
        )

        return checkpoints

    # ========================================================
    # LATEST CHECKPOINT
    # ========================================================

    def latest_checkpoint(
        self,
    ) -> Optional[RecoveryMetadata]:
        """Return the newest checkpoint."""

        checkpoints = (
            self.list_checkpoints()
        )

        if not checkpoints:

            return None

        return checkpoints[0]

    # ========================================================
    # GET CHECKPOINT
    # ========================================================

    def get_checkpoint(
        self,
        checkpoint_id: str,
    ) -> RecoveryMetadata:
        """Get checkpoint metadata by ID."""

        path = (
            self._checkpoint_path(
                checkpoint_id
            )
        )

        if not path.exists():

            raise CheckpointNotFoundError(
                "Checkpoint not found: "
                f"{checkpoint_id}"
            )

        return self._read_metadata(
            path
        )

    # ========================================================
    # VALIDATE CHECKPOINT
    # ========================================================

    def validate_checkpoint(
        self,
        checkpoint_id: str,
    ) -> RecoveryMetadata:
        """
        Validate a checkpoint.

        A valid checkpoint must contain:

            streamforge-recovery.json
            state.json
        """

        path = (
            self._checkpoint_path(
                checkpoint_id
            )
        )

        if not path.exists():

            raise CheckpointNotFoundError(
                "Checkpoint not found: "
                f"{checkpoint_id}"
            )

        try:

            metadata = (
                self._read_metadata(
                    path
                )
            )

            if (
                metadata.checkpoint_id
                != checkpoint_id
            ):

                raise InvalidCheckpointError(
                    "Checkpoint ID mismatch."
                )

            if not metadata.valid:

                raise InvalidCheckpointError(
                    "Checkpoint is marked invalid."
                )

            if not path.is_dir():

                raise InvalidCheckpointError(
                    "Checkpoint is not a directory."
                )

            state_file = (
                path / "state.json"
            )

            if not state_file.exists():

                raise InvalidCheckpointError(
                    "Checkpoint state.json is missing: "
                    f"{state_file}"
                )

            # ------------------------------------------------
            # Make sure state.json contains valid JSON.
            # ------------------------------------------------

            try:

                state_data = json.loads(
                    state_file.read_text(
                        encoding="utf-8"
                    )
                )

            except (
                OSError,
                json.JSONDecodeError,
            ) as exc:

                raise InvalidCheckpointError(
                    "Checkpoint state.json is invalid."
                ) from exc

            if not isinstance(
                state_data,
                dict,
            ):

                raise InvalidCheckpointError(
                    "Checkpoint state.json must "
                    "contain a JSON object."
                )

            self._statistics.checkpoints_validated += 1

            logger.info(
                "Checkpoint validated: %s",
                checkpoint_id,
            )

            return metadata

        except (
            CheckpointNotFoundError,
            InvalidCheckpointError,
        ):

            self._statistics.recovery_failures += 1

            raise

    # ========================================================
    # RESTORE CHECKPOINT
    # ========================================================

    def restore_checkpoint(
        self,
        checkpoint_id: Optional[str] = None,
        clear_existing: bool = True,
    ) -> int:
        """
        Restore checkpoint state into the existing StateStore.

        This method intentionally uses:

            self.state_store.restore_checkpoint(
                checkpoint_path,
                clear_existing
            )

        because that is the actual API exposed by state.py.

        Returns:
            Number of state entries restored.
        """

        start_time = time.perf_counter()

        # ----------------------------------------------------
        # Select checkpoint.
        # ----------------------------------------------------

        if checkpoint_id is None:

            latest = (
                self.latest_checkpoint()
            )

            if latest is None:

                raise CheckpointNotFoundError(
                    "No checkpoints are available."
                )

            checkpoint_id = (
                latest.checkpoint_id
            )

        # ----------------------------------------------------
        # Validate first.
        # ----------------------------------------------------

        metadata = (
            self.validate_checkpoint(
                checkpoint_id
            )
        )

        checkpoint_path = Path(
            metadata.checkpoint_path
        )

        logger.info(
            "Restoring checkpoint '%s'.",
            checkpoint_id,
        )

        try:

            # ------------------------------------------------
            # CORRECT API FROM state.py
            #
            # Signature:
            #
            # restore_checkpoint(
            #     self,
            #     checkpoint_path,
            #     clear_existing=True
            # ) -> int
            #
            # ------------------------------------------------

            restored_count = (
                self.state_store.restore_checkpoint(
                    checkpoint_path,
                    clear_existing=clear_existing,
                )
            )

            elapsed = (
                time.perf_counter()
                - start_time
            )

            self._statistics.checkpoints_restored += 1

            self._statistics.entries_restored += (
                restored_count
            )

            self._statistics.last_recovery_time = (
                time.time()
            )

            self._statistics.last_recovery_duration = (
                elapsed
            )

            logger.info(
                "Checkpoint restored successfully: "
                "checkpoint=%s entries=%d duration=%.4fs",
                checkpoint_id,
                restored_count,
                elapsed,
            )

            return int(
                restored_count
            )

        except Exception as exc:

            self._statistics.recovery_failures += 1

            logger.exception(
                "Failed to restore checkpoint '%s'.",
                checkpoint_id,
            )

            raise RecoveryError(
                f"Failed to restore checkpoint "
                f"'{checkpoint_id}': {exc}"
            ) from exc

    # ========================================================
    # RESTORE LATEST
    # ========================================================

    def restore_latest(
        self,
        clear_existing: bool = True,
    ) -> int:
        """Restore the latest checkpoint."""

        latest = (
            self.latest_checkpoint()
        )

        if latest is None:

            raise CheckpointNotFoundError(
                "No checkpoints are available."
            )

        return self.restore_checkpoint(
            checkpoint_id=(
                latest.checkpoint_id
            ),
            clear_existing=clear_existing,
        )

    # ========================================================
    # DELETE CHECKPOINT
    # ========================================================

    def delete_checkpoint(
        self,
        checkpoint_id: str,
    ) -> None:
        """Delete a managed checkpoint."""

        path = (
            self._checkpoint_path(
                checkpoint_id
            )
        )

        if not path.exists():

            raise CheckpointNotFoundError(
                "Checkpoint not found: "
                f"{checkpoint_id}"
            )

        shutil.rmtree(
            path
        )

        logger.info(
            "Deleted checkpoint: %s",
            checkpoint_id,
        )

    # ========================================================
    # RETENTION
    # ========================================================

    def cleanup_old_checkpoints(
        self,
    ) -> int:
        """
        Keep only the newest `retention` checkpoints.
        """

        checkpoints = (
            self.list_checkpoints()
        )

        deleted = 0

        for metadata in checkpoints[
            self.retention:
        ]:

            try:

                self.delete_checkpoint(
                    metadata.checkpoint_id
                )

                deleted += 1

            except (
                CheckpointNotFoundError,
                OSError,
            ):

                logger.warning(
                    "Unable to delete checkpoint: %s",
                    metadata.checkpoint_id,
                )

        return deleted

    # ========================================================
    # STATISTICS
    # ========================================================

    def statistics(
        self,
    ) -> Dict[str, Any]:
        """Return recovery statistics."""

        return {
            **self._statistics.to_dict(),
            "available_checkpoints": len(
                self.list_checkpoints()
            ),
            "checkpoint_directory": str(
                self.checkpoint_directory
            ),
            "retention": self.retention,
        }


# ============================================================
# CONVENIENCE FUNCTIONS
# ============================================================


def create_checkpoint(
    state_store: StateStore,
    checkpoint_directory: Optional[
        str | Path
    ] = None,
    checkpoint_id: Optional[str] = None,
) -> RecoveryMetadata:
    """Create a checkpoint using a temporary manager."""

    manager = RecoveryManager(
        state_store=state_store,
        checkpoint_directory=checkpoint_directory,
    )

    return manager.create_checkpoint(
        checkpoint_id=checkpoint_id
    )


def restore_checkpoint(
    state_store: StateStore,
    checkpoint_id: str,
    checkpoint_directory: Optional[
        str | Path
    ] = None,
    clear_existing: bool = True,
) -> int:
    """Restore a checkpoint into an existing StateStore."""

    manager = RecoveryManager(
        state_store=state_store,
        checkpoint_directory=checkpoint_directory,
    )

    return manager.restore_checkpoint(
        checkpoint_id=checkpoint_id,
        clear_existing=clear_existing,
    )


# ============================================================
# SELF TEST
# ============================================================


def run_self_test() -> None:

    print(
        "StreamForge recovery manager self-test"
    )

    print(
        "-" * 60
    )

    temporary_directory = Path(
        tempfile.mkdtemp(
            prefix="streamforge-recovery-test-"
        )
    )

    source_directory = (
        temporary_directory
        / "source"
    )

    checkpoint_directory = (
        temporary_directory
        / "checkpoints"
    )

    source_store = None

    try:

        # ====================================================
        # CREATE SOURCE STATE STORE
        # ====================================================

        source_store = StateStore(
            name="recovery-source",
            directory=source_directory,
        )

        # ----------------------------------------------------
        # Add test state.
        # ----------------------------------------------------

        source_store.set(
            "device-001",
            {
                "temperature": 25.5,
                "count": 10,
            },
        )

        source_store.set(
            "device-002",
            {
                "temperature": 30.0,
                "count": 20,
            },
        )

        source_store.set(
            "device-003",
            {
                "temperature": 22.5,
                "count": 15,
            },
        )

        source_count = len(
            list(
                source_store.keys()
            )
        )

        assert (
            source_count == 3
        )

        print(
            "Source state creation: OK"
        )

        # ====================================================
        # CREATE MANAGER
        # ====================================================

        manager = RecoveryManager(
            state_store=source_store,
            checkpoint_directory=checkpoint_directory,
            retention=3,
        )

        print(
            "Recovery manager creation: OK"
        )

        # ====================================================
        # CREATE CHECKPOINT
        # ====================================================

        checkpoint = (
            manager.create_checkpoint(
                checkpoint_id=(
                    "self-test-checkpoint"
                )
            )
        )

        assert (
            checkpoint.checkpoint_id
            == "self-test-checkpoint"
        )

        assert checkpoint.valid is True

        checkpoint_path = Path(
            checkpoint.checkpoint_path
        )

        assert checkpoint_path.exists()

        assert (
            checkpoint_path
            / "state.json"
        ).exists()

        print(
            "Checkpoint creation: OK"
        )

        # ====================================================
        # LIST CHECKPOINTS
        # ====================================================

        checkpoints = (
            manager.list_checkpoints()
        )

        assert len(
            checkpoints
        ) == 1

        assert (
            checkpoints[0].checkpoint_id
            == "self-test-checkpoint"
        )

        print(
            "Checkpoint discovery: OK"
        )

        # ====================================================
        # LATEST CHECKPOINT
        # ====================================================

        latest = (
            manager.latest_checkpoint()
        )

        assert latest is not None

        assert (
            latest.checkpoint_id
            == "self-test-checkpoint"
        )

        print(
            "Latest checkpoint lookup: OK"
        )

        # ====================================================
        # VALIDATION
        # ====================================================

        validated = (
            manager.validate_checkpoint(
                "self-test-checkpoint"
            )
        )

        assert validated.valid is True

        print(
            "Checkpoint validation: OK"
        )

        # ====================================================
        # MODIFY CURRENT STATE
        # ====================================================

        source_store.set(
            "device-004",
            {
                "temperature": 99.9,
                "count": 999,
            },
        )

        assert (
            len(
                list(
                    source_store.keys()
                )
            )
            == 4
        )

        print(
            "State modification before recovery: OK"
        )

        # ====================================================
        # RESTORE
        # ====================================================

        restored_count = (
            manager.restore_checkpoint(
                checkpoint_id=(
                    "self-test-checkpoint"
                ),
                clear_existing=True,
            )
        )

        assert (
            restored_count == 3
        )

        print(
            "Checkpoint restoration: OK"
        )

        # ====================================================
        # VERIFY RECOVERED STATE
        # ====================================================

        restored_keys = list(
            source_store.keys()
        )

        assert (
            len(restored_keys)
            == 3
        )

        assert (
            source_store.get(
                "device-001"
            )
            == {
                "temperature": 25.5,
                "count": 10,
            }
        )

        assert (
            source_store.get(
                "device-002"
            )
            == {
                "temperature": 30.0,
                "count": 20,
            }
        )

        assert (
            source_store.get(
                "device-003"
            )
            == {
                "temperature": 22.5,
                "count": 15,
            }
        )

        # device-004 was added after the checkpoint.
        # It must disappear because clear_existing=True.

        assert (
            source_store.get(
                "device-004"
            )
            is None
        )

        print(
            "State recovery verification: OK"
        )

        # ====================================================
        # STATISTICS
        # ====================================================

        statistics = (
            manager.statistics()
        )

        assert (
            statistics[
                "checkpoints_created"
            ]
            == 1
        )

        assert (
            statistics[
                "checkpoints_validated"
            ]
            >= 1
        )

        assert (
            statistics[
                "checkpoints_restored"
            ]
            == 1
        )

        assert (
            statistics[
                "entries_restored"
            ]
            == 3
        )

        print(
            "Recovery statistics: OK"
        )

        # ====================================================
        # SUCCESS
        # ====================================================

        print(
            "-" * 60
        )

        print(
            "StreamForge recovery manager "
            "self-test: OK"
        )

    finally:

        # ----------------------------------------------------
        # RocksDB must be closed before deleting temporary
        # directories, especially on Windows.
        # ----------------------------------------------------

        if source_store is not None:

            try:

                source_store.close()

            except Exception:

                pass

        # ----------------------------------------------------
        # Give Windows a short amount of time to release
        # the RocksDB LOCK file.
        # ----------------------------------------------------

        time.sleep(0.5)

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


if __name__ == "__main__":

    main()