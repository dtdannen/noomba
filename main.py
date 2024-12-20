import asyncio
import os
from dotenv import load_dotenv
from ezdvm import EZDVM
from nostr_sdk import Filter, Keys, Client, Timestamp, HandleNotification, PublicKey, Kind, EventBuilder
from datetime import timedelta
from loguru import logger

load_dotenv()


async def get_follows_list(npub:str, relays: [str]):
    users_pub_key = PublicKey.from_hex(npub)

    logger.warning(f"Getting follows list for user's npub: {npub} and hex: {users_pub_key.to_hex()}")

    client = Client()

    for relay in relays:
        logger.info(f"Adding relay: {relay}")
        await client.add_relay(relay)
    await client.connect()

    logger.info("Connected to relays")

    dvm_filter = Filter().authors([users_pub_key]).kinds([Kind(3)])
    #await client.subscribe([dvm_filter])

    events = await client.fetch_events([dvm_filter], timedelta(seconds=10))

    # keep the most recent one
    latest_follows_list = None
    latest_follows_list_timestamp = None
    for event in events.to_vec():
        valid_event = event.verify()
        if not valid_event:
            logger.warning(f"Invalid event: {event}")
            continue

        if latest_follows_list is None or event.created_at().as_secs() > latest_follows_list_timestamp.as_secs():
            num_follows = len([tag.as_vec()[1] for tag in event.tags().to_vec() if tag.as_vec()[0] == "p"])
            logger.info(f"Found later event and it has {num_follows}")
            latest_follows_list = event
            latest_follows_list_timestamp = event.created_at()

    if latest_follows_list:
        follows_list = [tag.as_vec()[1] for tag in latest_follows_list.tags().to_vec() if tag.as_vec()[0] == "p"]
        logger.info(f"final follows list was created on {latest_follows_list_timestamp}")
        for follow in follows_list[:5]:
            logger.info(f"Follow: {follow}")

        return follows_list


class Noomba(EZDVM):

    kinds = [5305]

    def __init__(self):
        # choose the job request kinds you will listen and respond to
        super().__init__(kinds=self.kinds)

    async def do_work(self, event):
        # create the signer for this DVM
        keys = Keys.generate()

        logger.info(f"NPUB: {keys.public_key()}")
        logger.info(f"NSEC: {keys.secret_key()}")

        # get user's npub from the event
        user_npub = event.author().to_hex()
        logger.info(f"User npub: {user_npub}")

        # get the relays on the note if they are there
        relays = os.getenv("DEFAULT_RELAYS").split(",")
        try:
            if event.relays():
                relays = [relay for relay in event.relays().to_vec()]
        except Exception as e:
            logger.error(f"Error getting relays from event, using default relays")

        # get the follows list
        follows_list = await get_follows_list(user_npub, relays)

        if follows_list:
            logger.info(f"Follows list has {len(follows_list)} follows")
            result_event_builder = EventBuilder.job_result(event, str(follows_list), 0)
            result_event = result_event_builder.sign_with_keys(keys)
        else:
            failed_event_build = EventBuilder.job_result(event, "Failed to get follows list for user", 0)
            result_event = failed_event_build.sign_with_keys(keys)

        return result_event


if __name__ == "__main__":
    hello_world_dvm = Noomba()
    hello_world_dvm.add_relay("wss://relay.damus.io")
    hello_world_dvm.add_relay("wss://relay.primal.net")
    hello_world_dvm.add_relay("wss://nos.lol")
    hello_world_dvm.add_relay("wss://nostr-pub.wellorder.net")
    hello_world_dvm.start()
