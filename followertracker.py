from asyncio import CancelledError
import nostr_sdk
from nostr_sdk import (
    Keys,
    PublicKey,
    Client,
    Filter,
    HandleNotification,
    Timestamp,
    LogLevel,
    NostrSigner,
    Kind,
    Event,
    NostrError,
    RelayMessage,
    EventBuilder,
    DataVendingMachineStatus,
    Tag,
    TagKind,
)
import os
import json
from loguru import logger
import asyncio
import traceback
from dotenv import load_dotenv
import sys

load_dotenv()


class FollowerTracker:

    def __init__(self, kinds=None, nsec_str=None, ephemeral=False):
        # Turn on nostr-sdk library logging
        nostr_sdk.init_logger(LogLevel.DEBUG)

        # Remove all existing handlers
        logger.remove()

        # Define a custom log format for console output
        console_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        )

        # Add a handler for console logging with the custom format
        logger.add(
            sys.stderr,
            format=console_format,
            level="DEBUG",
            colorize=True
        )

        # Add a handler for file logging
        logger.add(f"{self.__class__.__name__}.log", rotation="500 MB", level="DEBUG")

        self.logger = logger
        self.public_keys = self._get_starting_npubs()
        self.client = Client()
        self.job_queue = asyncio.Queue()
        self.finished_jobs = {}  # key is DVM Request event id, value is DVM Result event id

    def _get_starting_npubs(self, nsec_str=None, ephemeral=False):
        """
        Tries to get keys from env variable, then tries to get it from nsec_str arg, finally will generate if needed.
        :param nsec_str:
        :param do_not_save_nsec:
        :return:
        """
        npub_env_var_name = f"NPUB"
        public_key = PublicKey.from_bech32(os.getenv(npub_env_var_name, None))
        return [public_key]

    def add_relay(self, relay):
        asyncio.run(self.client.add_relay(relay))

    async def async_add_relay(self, relay):
        await self.client.add_relay(relay)

    def start(self):
        asyncio.run(self.async_start())

    async def async_start(self):
        self.logger.info("Connecting to relays...")
        await self.client.connect()
        self.logger.info("Successfully connected.")

        dvm_filter = Filter().kinds([Kind(3)]).authors(self.public_keys)
        await self.client.subscribe([dvm_filter])
        self.logger.info(f"Successfully subscribed.")

        class NotificationHandler(HandleNotification):
            def __init__(self, follower_tracker_instance):
                self.follower_tracker_instance = follower_tracker_instance
                self.relay_messages_counter = 0
                self.event_counter = 0

            async def handle(self, relay_url: str, subscription_id: str,event: Event):
                self.event_counter += 1
                self.follower_tracker_instance.logger.info(f"Received event {self.event_counter}")
                await self.follower_tracker_instance.job_queue.put(event)
                self.follower_tracker_instance.logger.info(f"Added event id {event.id().to_hex()} to job queue")
                #self.follower_tracker_instance.logger.info(f"View this DVM Request on DVMDash: https://dvmdash.live/event/{event.id().to_hex()}")

            async def handle_msg(self, relay_url: str, msg: RelayMessage):
                self.relay_messages_counter += 1
                try:
                    # Try to get the message contents only for more concise logging
                    msg_json = json.loads(msg.as_json())
                    #self.follower_tracker_instance.logger.info(f"Received message {self.relay_messages_counter} from {relay_url}: {msg_json}")
                except Exception as e:
                    self.follower_tracker_instance.logger.error(f"An error occurred: {str(e)}")
                    # if it fails, just log entire message json
                    #self.follower_tracker_instance.logger.info(
                    #    f"Received message {self.relay_messages_counter} from {relay_url}: {msg}")

        process_queue_task = asyncio.create_task(self.process_events_off_queue())

        handler = NotificationHandler(self)
        # Create a task for handle_notifications instead of awaiting it
        handle_notifications_task = asyncio.create_task(
            self.client.handle_notifications(handler)
        )

        try:
            await asyncio.gather(
                self.process_events_off_queue(),
                self.client.handle_notifications(handler)
            )
        except CancelledError:
            self.logger.info("Tasks cancelled, shutting down...")
        except Exception as e:
            self.logger.error(f"An error occurred: {str(e)}")
            self.logger.error(traceback.format_exc())
        finally:
            await self.shutdown()

    async def process_events_off_queue(self):
        while True:
            #logger.info(f"Job Queue has {self.job_queue.qsize()} events")
            try:
                # Wait for the first event or until max_wait_time
                event = await asyncio.wait_for(
                    self.job_queue.get(), timeout=1)

                #self.logger.info(f"Received event {event} from job queue")

                if event.kind() == Kind(3):
                    # collect all tags that are 'p' tags
                    tags = [tag for tag in event.tags() if tag.as_vec()[0] == "p"]
                    npubs = [tag.as_vec()[1] for tag in tags]

                    # print the first 5 and last 5 npubs, one on each line
                    for npub in npubs[:5]:
                        self.logger.info(f"First 5 npubs: {npub}")

                    for npub in npubs[-5:]:
                        self.logger.info(f"Last 5 npubs: {npub}")

                # request_event_id_as_hex = event.id().to_hex()
                #
                # if event and request_event_id_as_hex not in self.finished_jobs.keys():
                #     self.logger.info(f"Announcing to consumer/user that we are now processing"
                #                      f" the event: {request_event_id_as_hex}")
                #     processing_msg_event = await self.announce_status_processing(event)
                #     self.logger.info(f"Successfully sent 'processing' status event"
                #                      f" with id: {processing_msg_event.id().to_hex()}")
                #
                #     self.logger.info(f"Starting to work on event {request_event_id_as_hex}")
                #     content = await self.do_work(event)
                #     self.logger.info(f"Results from do_work() function are: {content}")
                #     self.logger.info(f"Broadcasting DVM Result event with the new results...")
                #     dvm_result_event = await self.send_dvm_result(event, content)
                #     result_event_id_as_hex = dvm_result_event.id().to_hex()
                #     self.logger.info(f"Successfully sent DVM Result event with id: {result_event_id_as_hex}")
                #     self.logger.info(f"View this DVM Result on "
                #                      f"DVMDash: https://dvmdash.live/event/{result_event_id_as_hex}")
                #
                #     self.finished_jobs[request_event_id_as_hex] = result_event_id_as_hex
                # else:
                #     self.logger.info(f"Skipping DVM Request {event.id().to_hex()}, we already did this,"
                #                      f" see DVM Result event: {self.finished_jobs[request_event_id_as_hex]}")

            except asyncio.TimeoutError:
                # If no events received within max_wait_time, continue to next iteration
                continue

            await asyncio.sleep(0.0001)

    async def shutdown(self):
        self.logger.info("Shutting down EZDVM...")
        await self.client.disconnect()
        self.logger.info("Client disconnected")


if __name__ == "__main__":
    follower_tracker = FollowerTracker()
    follower_tracker.add_relay("wss://relay.damus.io")
    follower_tracker.add_relay("wss://nostr.wine")
    follower_tracker.add_relay("wss://relay.nostr.info")
    follower_tracker.start()






