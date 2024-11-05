import aiosqlite
from nostr_sdk import (
    Keys,
    Client,
    Filter,
    HandleNotification,
    Timestamp,
    LogLevel,
    NostrSigner,
    Kind,
    Event,
    NostrError,
    PublicKey,
    RelayMessage
)
import os
import json
from loguru import logger
import asyncio
from typing import Set, List, Dict, Any
from datetime import datetime, timedelta
import traceback


class FollowerTracker:
    def __init__(self, db_path: str = 'noomba.db'):
        self.db_path = db_path
        self.db: aiosqlite.Connection = None
        self.client: Client = None
        self.processed_users: Set[str] = set()
        self.user_queue: asyncio.Queue = asyncio.Queue()
        self.rate_limit_delay = 5  # seconds between processing each user
        self.max_concurrent_users = 10

    async def setup(self):
        """Initialize database and nostr client"""
        self.db = await aiosqlite.connect(self.db_path)
        await self._setup_tables()
        self.client = await self._setup_nostr_client()

    async def _setup_tables(self):
        """Setup database tables"""
        await self.db.execute('''
        CREATE TABLE IF NOT EXISTS followers (
            follower TEXT NOT NULL,
            followee TEXT NOT NULL,
            unfollowed BOOLEAN NOT NULL DEFAULT 0,
            last_event_seen TIMESTAMP,
            PRIMARY KEY (follower, followee)
        )
        ''')

        await self.db.execute('''
        CREATE TABLE IF NOT EXISTS processed_users (
            npub TEXT PRIMARY KEY,
            last_processed TIMESTAMP,
            next_scheduled_update TIMESTAMP
        )
        ''')

        await self.db.commit()

    async def _setup_nostr_client(self) -> Client:
        """Setup and connect nostr client"""
        relays = self._get_relays()
        client = Client()

        logger.info("Connecting to relays...")
        for relay in relays:
            await client.add_relay(relay)
            logger.info(f"Added relay {relay}")
        await client.connect()
        logger.info("Successfully connected.")

        return client

    def _get_relays(self) -> List[str]:
        """Get relay list from environment variable"""
        return os.getenv(
            "RELAYS",
            "wss://relay.damus.io,wss://blastr.f7z.xyz,wss://relayable.org,wss://purplepag.es"
        ).split(",")

    async def add_user_to_queue(self, npub: str):
        """Add a user to the processing queue if not recently processed"""
        logger.info(f"Attempting to add user {npub} for processing...")
        async with self.db.execute(
                "SELECT last_processed, next_scheduled_update FROM processed_users WHERE npub = ?",
                (npub,)
        ) as cursor:
            result = await cursor.fetchone()

            current_time = datetime.now()
            if not result or (
                    datetime.fromisoformat(result[1]) <= current_time
            ):
                await self.user_queue.put(npub)
                logger.info(f"Added {npub} to processing queue")

    async def process_user_followers(self, npub: str):
        """Process followers for a single user"""
        try:
            logger.info(f"Processing user {npub}...")
            user_pubkey = PublicKey.from_bech32(npub)
            follow_filter = Filter().kinds([Kind(3)]).authors([user_pubkey])
            logger.info(f"Processing user {npub} with filter {follow_filter}")

            # Create a temporary queue for this user's events
            event_queue = asyncio.Queue()

            # Create a dedicated notification handler for this subscription
            class UserNotificationHandler(HandleNotification):
                async def handle(self_, relay_url: str, subscription_id: str, event: Event):
                    logger.info(f"Received event for {npub}: {event.id().to_hex()}")
                    await event_queue.put(event)

                async def handle_msg(self_, relay_url: str, msg: RelayMessage):
                    logger.info(f"Received relay message for {npub}: {msg}")

            # Subscribe and create handler
            handler = UserNotificationHandler()
            subscription_task = asyncio.create_task(self.client.handle_notifications(handler))

            logger.info(f"Creating subscription for {npub}")
            await self.client.subscribe([follow_filter])
            logger.info(f"Subscription created for {npub}")

            # Process events for up to 30 seconds
            try:
                async with asyncio.timeout(30):  # Use timeout instead of sleep
                    while True:
                        event = await event_queue.get()
                        await self._process_kind_3_event(event)
                        event_queue.task_done()
            except TimeoutError:
                logger.info(f"Finished processing {npub} (timeout)")
            finally:
                # Cleanup subscription
                subscription_task.cancel()
                try:
                    await subscription_task
                except asyncio.CancelledError:
                    pass

            # Update processed_users table
            next_update = datetime.now() + timedelta(hours=24)
            await self.db.execute('''
                INSERT OR REPLACE INTO processed_users (npub, last_processed, next_scheduled_update)
                VALUES (?, CURRENT_TIMESTAMP, ?)
            ''', (npub, next_update.isoformat()))
            await self.db.commit()

        except Exception as e:
            logger.error(f"Error processing user {npub}: {e}")
            logger.error(traceback.format_exc())

    async def _process_kind_3_event(self, event: Event):
        """Process a single kind 3 event"""
        async with self.db.cursor() as cur:
            follower_pubkey = event.author.to_hex()

            # Extract followees from event tags
            followees = [tag[1] for tag in event.tags if tag[0] == 'p']

            # Get current followees
            await cur.execute(
                "SELECT followee FROM followers WHERE follower = ? AND unfollowed = 0",
                (follower_pubkey,)
            )
            current_followees = set(row[0] for row in await cur.fetchall())

            # Process new follows
            for followee in followees:
                if followee not in current_followees:
                    await cur.execute('''
                        INSERT OR REPLACE INTO followers 
                        (follower, followee, unfollowed, last_event_seen)
                        VALUES (?, ?, 0, CURRENT_TIMESTAMP)
                    ''', (follower_pubkey, followee))
                    # Add new followee to processing queue
                    await self.add_user_to_queue(followee)

            # Process unfollows
            for unfollowed in current_followees - set(followees):
                await cur.execute('''
                    UPDATE followers 
                    SET unfollowed = 1, last_event_seen = CURRENT_TIMESTAMP
                    WHERE follower = ? AND followee = ?
                ''', (follower_pubkey, unfollowed))

            await self.db.commit()

    async def process_queue_worker(self):
        """Worker to process users from the queue"""
        while True:
            try:
                npub = await self.user_queue.get()
                if npub not in self.processed_users:
                    await self.process_user_followers(npub)
                    self.processed_users.add(npub)
                    self.user_queue.task_done()
                    await asyncio.sleep(self.rate_limit_delay)
            except asyncio.CancelledError:
                logger.info("Worker cancelled, shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in queue worker: {e}")
                logger.error(traceback.format_exc())
                # Don't break on error, continue processing other items
                continue

    async def run(self, initial_npubs: List[str]):
        """Main run method"""
        # Add initial users to queue
        for npub in initial_npubs:
            await self.add_user_to_queue(npub)

        # Start workers
        workers = [
            asyncio.create_task(self.process_queue_worker())
            for _ in range(self.max_concurrent_users)
        ]

        try:
            # Just wait for the queue to be processed
            await self.user_queue.join()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            # Cancel workers
            for worker in workers:
                worker.cancel()
            try:
                await asyncio.gather(*workers, return_exceptions=True)
            except asyncio.CancelledError:
                pass
            # Cleanup
            await self.cleanup()

    async def print_db_summary(self):
        """Print a summary of the database"""
        async with self.db.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM followers")
            follower_count = await cur.fetchone()
            await cur.execute("SELECT COUNT(*) FROM processed_users")
            processed_count = await cur.fetchone()
        logger.info(f"Database summary: {follower_count[0]} followers, {processed_count[0]} processed users")

    async def cleanup(self):
        """Cleanup resources"""
        if self.db:
            await self.db.close()
        if self.client:
            await self.client.disconnect()


async def main():
    # Initial list of npubs to start with
    initial_npubs = [
        "npub1mgvwnpsqgrem7jfcwm7pdvdfz2h95mm04r23t8pau2uzxwsdnpgs0gpdjc",
        # Add more initial npubs...
    ]

    tracker = FollowerTracker()
    await tracker.setup()
    await tracker.print_db_summary()
    await tracker.run(initial_npubs)


if __name__ == "__main__":
    asyncio.run(main())