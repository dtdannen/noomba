import numpy as np
import json
import time
from pathlib import Path
import logging
from typing import Dict, Set, Optional
import scipy.sparse as sparse
import threading
import queue


class PersistentFollowerGraph:
    def __init__(self, backup_dir: str, backup_interval_seconds: int = 300):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        # Core data structures
        self.user_to_index: Dict[str, int] = {}
        self.index_to_user: Dict[int, str] = {}
        self.next_index: int = 0

        # Use sparse matrix for memory efficiency
        self.adjacency_matrix = sparse.lil_matrix((1000, 1000), dtype=np.int8)

        # Setup backup system
        self.backup_interval = backup_interval_seconds
        self.last_backup_time = time.time()
        self.changes_since_backup = 0
        self.backup_threshold = 1000  # Backup after this many changes

        # Queue for tracking changes that need to be persisted
        self.change_queue = queue.Queue()

        # Load existing data if available
        self._load_latest_backup()

        # Start backup thread
        self.backup_thread = threading.Thread(target=self._backup_worker, daemon=True)
        self.backup_thread.start()

        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _backup_worker(self):
        """Background thread that handles periodic backups."""
        while True:
            try:
                # Process any pending changes
                while not self.change_queue.empty():
                    change = self.change_queue.get_nowait()
                    self._process_change(change)

                current_time = time.time()
                # Backup if enough time has passed or enough changes accumulated
                if (current_time - self.last_backup_time > self.backup_interval or
                        self.changes_since_backup > self.backup_threshold):
                    self._create_backup()

                time.sleep(1)  # Sleep to prevent tight loop
            except Exception as e:
                self.logger.error(f"Backup worker error: {e}")

    def _process_change(self, change):
        """Process a change from the queue."""
        operation, follower, following = change
        if operation == 'add':
            self._add_follow_internal(follower, following)
        elif operation == 'remove':
            self._remove_follow_internal(follower, following)

    def add_follow(self, follower: str, following: str):
        """Thread-safe method to add a follow relationship."""
        self.change_queue.put(('add', follower, following))
        self.changes_since_backup += 1

    def remove_follow(self, follower: str, following: str):
        """Thread-safe method to remove a follow relationship."""
        self.change_queue.put(('remove', follower, following))
        self.changes_since_backup += 1

    def _add_follow_internal(self, follower: str, following: str):
        """Internal method to add follow relationship."""
        # Ensure users are in our mappings
        for user in [follower, following]:
            if user not in self.user_to_index:
                self.user_to_index[user] = self.next_index
                self.index_to_user[self.next_index] = user
                self.next_index += 1

                # Expand matrix if needed
                if self.next_index > self.adjacency_matrix.shape[0]:
                    new_size = max(self.next_index * 2, 1000)  # Double size or min 1000
                    new_matrix = sparse.lil_matrix((new_size, new_size), dtype=np.int8)
                    new_matrix[:self.adjacency_matrix.shape[0], :self.adjacency_matrix.shape[0]] = self.adjacency_matrix
                    self.adjacency_matrix = new_matrix

        # Set the follow relationship
        follower_idx = self.user_to_index[follower]
        following_idx = self.user_to_index[following]
        self.adjacency_matrix[follower_idx, following_idx] = 1

    def _remove_follow_internal(self, follower: str, following: str):
        """Internal method to remove follow relationship."""
        if follower in self.user_to_index and following in self.user_to_index:
            follower_idx = self.user_to_index[follower]
            following_idx = self.user_to_index[following]
            self.adjacency_matrix[follower_idx, following_idx] = 0

    def _create_backup(self):
        """Create a backup of the current state."""
        try:
            timestamp = int(time.time())

            # Save user mappings
            mappings_file = self.backup_dir / f"user_mappings_{timestamp}.json"
            with open(mappings_file, 'w') as f:
                json.dump({
                    'user_to_index': self.user_to_index,
                    'next_index': self.next_index
                }, f)

            # Save sparse matrix in npz format
            matrix_file = self.backup_dir / f"adjacency_matrix_{timestamp}.npz"
            sparse.save_npz(matrix_file, self.adjacency_matrix.tocsr())

            # Update backup time and reset change counter
            self.last_backup_time = time.time()
            self.changes_since_backup = 0

            # Cleanup old backups (keep last 3)
            self._cleanup_old_backups()

            self.logger.info(f"Backup created: {timestamp}")

        except Exception as e:
            self.logger.error(f"Backup creation failed: {e}")

    def _load_latest_backup(self):
        """Load the most recent backup if available."""
        try:
            # Find latest backup files
            matrix_files = list(self.backup_dir.glob("adjacency_matrix_*.npz"))
            mapping_files = list(self.backup_dir.glob("user_mappings_*.json"))

            if not matrix_files or not mapping_files:
                self.logger.info("No existing backup found")
                return

            # Get latest backup
            latest_matrix = max(matrix_files, key=lambda x: int(x.stem.split('_')[-1]))
            latest_mapping = max(mapping_files, key=lambda x: int(x.stem.split('_')[-1]))

            # Load mappings
            with open(latest_mapping, 'r') as f:
                data = json.load(f)
                self.user_to_index = data['user_to_index']
                self.index_to_user = {int(v): k for k, v in self.user_to_index.items()}
                self.next_index = data['next_index']

            # Load matrix
            self.adjacency_matrix = sparse.load_npz(latest_matrix)
            self.adjacency_matrix = self.adjacency_matrix.tolil()  # Convert to LIL format for efficient updates

            self.logger.info(f"Loaded backup from {latest_matrix.stem}")

        except Exception as e:
            self.logger.error(f"Backup loading failed: {e}")

    def _cleanup_old_backups(self, keep_last_n: int = 3):
        """Clean up old backups, keeping only the n most recent."""
        try:
            matrix_files = list(self.backup_dir.glob("adjacency_matrix_*.npz"))
            mapping_files = list(self.backup_dir.glob("user_mappings_*.json"))

            # Sort by timestamp
            matrix_files.sort(key=lambda x: int(x.stem.split('_')[-1]), reverse=True)
            mapping_files.sort(key=lambda x: int(x.stem.split('_')[-1]), reverse=True)

            # Remove old files
            for f in matrix_files[keep_last_n:]:
                f.unlink()
            for f in mapping_files[keep_last_n:]:
                f.unlink()

        except Exception as e:
            self.logger.error(f"Backup cleanup failed: {e}")


# Example usage
if __name__ == "__main__":
    graph = PersistentFollowerGraph("./backups", backup_interval_seconds=10)

    # Simulate some follow/unfollow activity
    graph.add_follow("npub1", "npub2")
    graph.add_follow("npub2", "npub3")
    time.sleep(5)
    graph.add_follow("npub1", "npub3")
    graph.remove_follow("npub1", "npub2")

    # Keep main thread alive to see backups happen
    time.sleep(15)