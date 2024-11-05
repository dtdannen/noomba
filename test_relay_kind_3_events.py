from nostr_sdk import Filter, Alphabet, Keys, SingleLetterTag, Kind, KindEnum

import asyncio

from nostr_sdk import Client

from dotenv import load_dotenv

load_dotenv()




async def main():
    client = Client()

    await client.add_relay("wss://relay.damus.io")
    await client.add_relay("wss://nostr.wine")
    await client.add_relay("wss://relay.nostr.info")
    await client.connect()

    relays = await client.relays()
    for url, relay in relays.items():
        stats = relay.stats()
        print(f"Relay: {url}")
        print(f"Connected: {await relay.is_connected()}")
        print(f"Status: {await relay.status()}")
        print("Stats:")
        print(f"    Attempts: {stats.attempts()}")
        print(f"    Success: {stats.success()}")
        print(f"    Bytes sent: {stats.bytes_sent()}")
        print(f"    Bytes received: {stats.bytes_received()}")
        print(f"    Connected at: {stats.connected_at().to_human_datetime()}")

        latency = await stats.latency()
        if latency is not None:
            print(f"    Latency: {latency.total_seconds() * 1000} ms")

        print("###########################################")

    f = (Filter()
         .pubkey(keys.public_key())
         .kinds([Kind(3)]))



if __name__ == '__main__':
    asyncio.run(main())



print(f.as_json())

