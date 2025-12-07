from typing import List, Optional, Set
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorDatabase,
    AsyncIOMotorCollection
)

from pymongo.errors import CollectionInvalid
from source.Logging import Logger

from source.Database.Models import UserModel, ChannelModel
from source.TelegramMessageScrapper.PyroClient import PyroClient
# from source.ChromaАndRAG.ChromaClient import RagClient


class DataBaseHelper:
    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        scrapper: Optional[PyroClient]
    ):
        self.mongo_db_logger = Logger("MongoDB", "network.log")
        self.db = db
        self.users: AsyncIOMotorCollection = db["users"]
        self.channels: AsyncIOMotorCollection = db["channels"]
        self.scrapper = scrapper

    @classmethod
    async def create(
        cls,
        uri: str = "",
        db_name: str = "",
        scrapper: PyroClient = None  # Add scrapper argument
    ) -> "DataBaseHelper":

        client = AsyncIOMotorClient(uri)
        db = client[db_name]
        self = cls(db, scrapper)  # Pass scrapper to the constructor
        await self._setup()
        await self.mongo_db_logger.info("MongoDB connected")
        return self

    async def _setup(self) -> None:
        collections = await self.db.list_collection_names()
        if "users" not in collections:
            try:
                await self.db.create_collection("users")
            except CollectionInvalid:
                await self.mongo_db_logger.warning(
                    "Collection 'users' already exists"
                )
        if "channels" not in collections:
            try:
                await self.db.create_collection("channels")
            except CollectionInvalid:
                await self.mongo_db_logger.warning(
                    "Collection 'channels' already exists"
                )

    async def create_user(self, user_id: int, name: str) -> None:
        if await self.users.find_one({"_id": user_id}):
            await self.mongo_db_logger.warning(
                f"User '{user_id}' already exists"
            )
            raise ValueError("User already exists")
        user = UserModel(
            id=user_id,
            name=name
        )
        await self.users.insert_one(user.dict(by_alias=True))

    async def delete_user(self, user_id: int) -> list:
        channels_to_unsubscribe = []
        user_doc = await self.users.find_one({"_id": user_id})
        if not user_doc:
            await self.mongo_db_logger.warning(f"User '{user_id}' not found")
            raise ValueError("User not found")
        user = UserModel(**user_doc)
        for channel_id in user.channels:
            res = await self._decrement_channel(channel_id)
            if res:
                channels_to_unsubscribe.append(channel_id)

        await self.users.delete_one({"_id": user_id})
        return channels_to_unsubscribe

    async def update_user_channels(
        self,
        user_id: int,
        add: Optional[List[int]] = None,
        remove: Optional[List[int]] = None
    ) -> list:
        channels_to_unsubscribe = []
        doc = await self.users.find_one({"_id": user_id})
        if not doc:
            raise ValueError("User not found")
        user = UserModel(**doc)
        current: Set[int] = set(user.channels)
        to_add = set(add or [])
        to_remove = set(remove or [])
        for ch in to_add:
            if not await self.channels.find_one({"_id": ch}):
                raise ValueError(f"Channel {ch} does not exist")
        for ch in to_add - current:
            await self._increment_channel(ch)
        for ch in to_remove & current:
            res = await self._decrement_channel(ch)
            if res:
                channels_to_unsubscribe.append(ch)
        updated = (current | to_add) - to_remove
        user.channels = list(updated)
        await self.users.replace_one(
            {"_id": user_id},
            user.dict(by_alias=True)
        )
        return channels_to_unsubscribe

    async def get_user(self, user_id: int) -> UserModel:
        doc = await self.users.find_one({"_id": user_id})
        if not doc:
            raise ValueError("User not found")
        return UserModel(**doc)

    async def create_channel(self, channel_id: int, name: str) -> None:
        if await self.channels.find_one({"_id": channel_id}):
            await self.mongo_db_logger.warning(
                f"Channel '{channel_id}' already exists. Will not create one."
            )
            raise ValueError("Channel already exists")
        channel = ChannelModel(id=channel_id, name=name)
        await self.channels.insert_one(channel.dict(by_alias=True))

    async def delete_channel(self, channel_id: int) -> None:
        doc = await self.channels.find_one({"_id": channel_id})
        if not doc:
            raise ValueError("Channel not found")

        if doc["subscribers"] > 0:
            raise ValueError("Channel has subscribers")

        await self.channels.delete_one({"_id": channel_id})

    async def get_channel(self, channel_id: int) -> ChannelModel:
        doc = await self.channels.find_one({"_id": channel_id})
        if not doc:
            raise ValueError("Channel not found")
        return ChannelModel(**doc)

    async def _increment_channel(self, channel_id: int) -> None:
        await self.channels.update_one(
            {"_id": channel_id},
            {"$inc": {"subscribers": 1}}
        )

    async def _decrement_channel(self, channel_id: int) -> bool:
        to_remove = False
        doc = await self.channels.find_one({"_id": channel_id})
        if not doc:
            return to_remove

        if doc["subscribers"] <= 1:
            await self.channels.delete_one({"_id": channel_id})
            to_remove = True
            return to_remove
        else:
            await self.channels.update_one(
                {"_id": channel_id},
                {"$inc": {"subscribers": -1}}
            )
            return to_remove
