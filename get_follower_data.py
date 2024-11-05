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


async def setup_db():
    # Connect to a database (creates it if it doesn't exist)
    db = await aiosqlite.connect('noomba.db')

    # Create the followers table
    await db.execute('''
    CREATE TABLE IF NOT EXISTS followers (
        follower TEXT NOT NULL,
        followee TEXT NOT NULL,
        unfollowed BOOLEAN NOT NULL DEFAULT 0,
        last_event_seen TIMESTAMP,
        PRIMARY KEY (follower, followee)
    )
    ''')

    await db.commit()

    return db


async def setup_nostr_client():
    relays = get_relays()

    client = Client()

    logger.info("Connecting to relays...")
    for relay in relays:
        await client.add_relay(relay)
        logger.info(f"Added relay {relay}")
    await client.connect()
    logger.info("Successfully connected.")

    return client


async def cleanup(db):
    # Close the aiosqlite connection
    await db.close()


async def get_relays():
    relays = os.getenv(
        "RELAYS",
        "wss://relay.damus.io,wss://blastr.f7z.xyz,wss://relayable.org,wss://nostr-pub.wellorder.net,wss://purplepag.es",
    ).split(",")
    return relays


async def subscribe_for_followers(client, conn, user_npub_hex_list):
    logger.info(f"Subscribing to follow {len(user_npub_hex_list)} users...")
    user_pub_hex_list = [PublicKey.from_hex(u) for u in user_npub_hex_list]

    follow_filter = Filter().kinds([Kind(3)]).authors(user_pub_hex_list)

    await client.subscribe([follow_filter])
    logger.info(f"Successfully subscribed to {len(user_npub_hex_list)} users.")

    class NotificationHandler(HandleNotification):
        def __init__(self):
            pass

        async def handle(self, relay_url: str, subscription_id: str,event: Event):
            logger.info(f"Received event from {relay_url}: {event}")

            # Save the event to the database
            await self.insert_kind3_event(conn, event, event.author)

        async def handle_msg(self, relay_url: str, msg: RelayMessage):
            try:
                # Try to get the message contents only for more concise logging
                msg_json = json.loads(msg.as_json())
                logger.info(f"Received message from {relay_url}: {msg_json}")
            except Exception as e:
                # if it fails, just log entire message json
                logger.info(
                    f"Received message from {relay_url}: {msg}")

        async def process_kind_3_event(self, db: aiosqlite.Connection, event: Dict[str, Any], follower_pubkey: str):
            async with db.cursor() as cur:

                # Get current followees from the database
                await cur.execute("SELECT followee FROM followers WHERE follower = ? AND unfollowed = 0",
                                  (follower_pubkey,))
                current_followees = set(row[0] for row in await cur.fetchall())

                # Get new followees from the event
                new_followees = set(tag[1] for tag in event['tags'] if tag[0] == 'p')

                # Prepare SQL statements
                insert_sql = '''
                INSERT OR REPLACE INTO followers 
                (follower, followee, unfollowed, last_event_seen)
                VALUES (?, ?, 0, CURRENT_TIMESTAMP)
                '''

                unfollow_sql = '''
                UPDATE followers 
                SET unfollowed = 1, last_event_seen = CURRENT_TIMESTAMP
                WHERE follower = ? AND followee = ?
                '''

                # Process new and existing followees
                for followee in new_followees:
                    await cur.execute(insert_sql, (follower_pubkey, followee))

                # Process unfollows
                for unfollowed in current_followees - new_followees:
                    await cur.execute(unfollow_sql, (follower_pubkey, unfollowed))

                # Commit the transaction
                await db.commit()

    process_queue_task = asyncio.create_task(process_events_off_queue())

    handler = NotificationHandler(self)
    # Create a task for handle_notifications instead of awaiting it
    handle_notifications_task = asyncio.create_task(
        client.handle_notifications(handler)
    )

    try:
        await asyncio.gather(
            process_events_off_queue(),
            client.handle_notifications(handler)
        )
    except CancelledError:
        logger.info("Tasks cancelled, shutting down...")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        await shutdown()



def main():
    conn = setup_db()
    client = setup_nostr_client()
    cleanup(conn)


if __name__ == "__main__":
    main()

