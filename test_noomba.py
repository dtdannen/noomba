import asyncio
from datetime import timedelta
from nostr_sdk import *
from dotenv import load_dotenv
import os

load_dotenv()


async def main():
    signer = Keys.parse(os.getenv("DUSTINS_NSEC"))

    client = Client(signer)

    await client.add_relay("wss://relay.damus.io")
    await client.add_relay("wss://relay.primal.net")
    await client.add_relay("wss://nos.lol")
    await client.add_relay("wss://nostr-pub.wellorder.net")

    await client.connect()

    builder = EventBuilder(kind=Kind(5305), content="", tags=[])

    request_event = await client.send_event_builder(builder)

    print(f"Request event: {request_event.id.to_hex()}")

    print("Getting events from relays that mention this event")

    filter = Filter().event(request_event.id)

    events = await client.fetch_events([filter], timedelta(seconds=10))
    for event in events.to_vec():
        print(f"Kind {event.kind()} event: {event.id()}")
        print(f"Content: {event.content()}")
        print(f"Tags: {[str(tag) for tag in event.tags().to_vec()]}")


if __name__ == '__main__':
    asyncio.run(main())