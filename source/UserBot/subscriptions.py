import re
from pyrogram import Client, errors


async def subscribe_to_channel(app: Client, channel_identifier: str) -> dict:
    """
    Subscribes to a channel by username, ID, or invitation link.

    Handles:
    - If the user is already subscribed → returns "already_subscribed".
    - If the subscription is successful → returns "success".
    - If a request for approval is sent → returns "request_sent".
    - If the channel is private and inaccessible → returns "private_channel".
    - If an error occurs → returns "error".
    """
    result = {"status": "", "description": ""}

    # Check if the parameter is a link or ID
    invite_match = re.match(r"https://t\.me/\+(\w+)",
                            channel_identifier)  # Invitation link
    normal_link_match = re.match(
        r"https://t\.me/([\w\d_]+)", channel_identifier)  # Regular link

    if invite_match:
        # Keep the invitation link
        channel_identifier = f"t.me/+{invite_match.group(1)}"
    elif normal_link_match:
        channel_identifier = normal_link_match.group(
            1)  # Take only the username
    elif channel_identifier.isdigit():  # If the str contains only digits—>ID
        # If the ID is incomplete, complete it
        if not channel_identifier.startswith("-100"):
            channel_identifier = f"-100{channel_identifier}"

    if not invite_match:
        # Check if the user is already subscribed
        try:
            await app.get_chat_member(channel_identifier, "me")
            result["status"] = "already_subscribed"
            result["description"] = \
                f"Already subscribed to {channel_identifier}"
            return result
        except errors.UserNotParticipant:
            pass
        except errors.UsernameInvalid:
            # If a link is provided,
            # this is normal, we will handle it in join_chat
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

        # Try to subscribe to the channel
        try:
            await app.join_chat(channel_identifier)
            result["status"] = "success"
            result["description"] = \
                f"Successfully subscribed to {channel_identifier}"
        except errors.UserAlreadyParticipant:
            result["status"] = "already_subscribed"
            result["description"] = \
                f"Already subscribed to {channel_identifier}"
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
                "The channel is private and cannot be accessed directly by ID."
        except Exception as e:
            result["status"] = "error"
            result["description"] = \
                f"Error subscribing to {channel_identifier}: {str(e)}"

        return result
