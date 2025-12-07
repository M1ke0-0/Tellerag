import asyncio
import re

from pyrogram import Client, errors


class PyroClient:
    def __init__(self, api_id: int, api_hash: str, history_limit: int):
        self.pyro_client = Client(
            name="TELERAG-MessageScrapper",
            api_id=api_id,
            api_hash=api_hash
        )
        self.message_hist_limit = history_limit

    async def scrapper_start(self):
        await self.pyro_client.start()

    async def scrapper_stop(self):
        await self.pyro_client.stop()

    async def subscribe_to_channel(
        self,
        channel_identifier: str
    ) -> dict:
        """
        Subscribes to a channel by username, ID, or invitation link.
        Handles:
        - If the user is already subscribed → returns "already_subscribed".
        - If the subscription is successful → returns "success" with channel
            ID and name.
        - If a request for approval is sent → returns "request_sent".
        - If the channel is private and inaccessible → returns "prvt_chnl".
        - If an error occurs → returns "error".
        """
        result = {
            "status": "",
            "description": "",
            "channel_id": None,
            "channel_name": None
            }

        invite_match = re.match(r"https://t\.me/\+(\w+)",
                                channel_identifier)
        normal_link_match = re.match(
            r"https://t\.me/([\w\d_]+)", channel_identifier)

        if invite_match:
            channel_identifier = f"t.me/+{invite_match.group(1)}"
        elif normal_link_match:
            channel_identifier = normal_link_match.group(1)
        elif channel_identifier.isdigit():
            if not channel_identifier.startswith("-100"):
                channel_identifier = f"-100{channel_identifier}"

        if not invite_match:
            try:
                chat = await self.pyro_client.get_chat(channel_identifier)
                await self.pyro_client.get_chat_member(
                    channel_identifier,
                    "me")
                result["status"] = "already_subscribed"
                result["description"] = \
                    f"Already subscribed to {channel_identifier}"
                result["channel_id"] = chat.id
                result["channel_name"] = chat.title
                return result
            except errors.UserNotParticipant:
                pass
            except errors.UsernameInvalid:
                pass
            except errors.PeerIdInvalid:
                result["status"] = "error"
                result["description"] = \
                    "Invalid channel ID. Make sure it's correct."
                return result
            except errors.ChannelPrivate:
                result["status"] = "private_channel"
                result["description"] = \
                    "The channel is private " + \
                    "and cannot be accessed directly by ID." + \
                    "Please provide an invite link."
                return result
            except Exception as e:
                result["status"] = "error"
                result["description"] = \
                    f"Error checking subscription: {str(e)}"
                return result

            try:
                chat = await self.pyro_client.join_chat(channel_identifier)
                result["status"] = "success"
                result["description"] = \
                    f"Successfully subscribed to {channel_identifier}"
                result["channel_id"] = chat.id
                result["channel_name"] = chat.title
            except errors.UserAlreadyParticipant:
                chat = await self.pyro_client.get_chat(channel_identifier)
                result["status"] = "already_subscribed"
                result["description"] = \
                    f"Already subscribed to {channel_identifier}"
                result["channel_id"] = chat.id
                result["channel_name"] = chat.title
            except errors.InviteRequestSent:
                result["status"] = "request_sent"
                result["description"] = \
                    f"Request to join {channel_identifier} sent for approval"
            except errors.PeerIdInvalid:
                result["status"] = "error"
                result["description"] = "\
                    Invalid channel ID. Make sure it's correct."
            except errors.ChannelPrivate:
                result["status"] = "private_channel"
                result["description"] = \
                    (
                        "The channel is private and cannot be "
                        "accessed directly by ID."
                )
            except Exception as e:
                result["status"] = "error"
                result["description"] = \
                    f"Error subscribing to {channel_identifier}: {str(e)}"

            return result

    async def unsubscribe_from_channel(self, channel_identifier: str):
        try:
            await self.pyro_client.leave_chat(str(channel_identifier))
            return {
                "status": "success",
                "description": f"Unsubscribed from {channel_identifier}"
            }
        except Exception:
            return {
                "status": "error",
                "description": f"Error unsubscribing from {channel_identifier}"
            }

    async def fetch(self, channel_identifier: str):
        """
        Fetches the messages from the channel.
        """
        msgs = []
        try:
            async for message in self.pyro_client.get_chat_history(
                channel_identifier,
                limit=100
            ):
                if message.caption or message.text:
                    msgs.append(
                        {
                            "post_id": message.id,
                            "text": message.caption or message.text
                        }
                    )

                if len(msgs) >= self.message_hist_limit:
                    break
        except errors.FloodWait as e:
            print(f"Flood wait: {e.x} seconds")
            await asyncio.sleep(e.x)

        return msgs
