from typing import Optional

from aiogram.client.default import DefaultBotProperties
from aiogram import Bot, Dispatcher, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    BotCommand,
    ReplyKeyboardMarkup,
    KeyboardButton,
)

from source.TgUI.States import AddSourceStates
from source.Logging import Logger
from source.Database.DBHelper import DataBaseHelper
from source.ChromaАndRAG.Rag import RagClient
from source.TelegramMessageScrapper.PyroClient import PyroClient
import asyncio


class BotApp:
    def __init__(
        self, token: str,
        db_helper: Optional[DataBaseHelper],
        scrapper: Optional[PyroClient],
        rag: RagClient
    ):
        self.telegram_ui_logger = Logger("TelegramUI", "network.log")
        self.bot = Bot(
            token=token,
            default=DefaultBotProperties(
                parse_mode="HTML",
            )
        )
        self.dispatcher = Dispatcher(storage=MemoryStorage())
        self.router = Router()
        self.dispatcher.include_router(self.router)
        self.__include_handlers()

        self.DataBaseHelper = db_helper
        self.RagClient = rag
        self.Scrapper = scrapper

    def include_db(self, db_helper: DataBaseHelper):
        if self.DataBaseHelper is None:
            self.DataBaseHelper = db_helper

    def __include_handlers(self):
        # --- Хэндлеры для сообщений ---
        self.router.message.register(self.__start_handler, F.text == "/start")
        self.router.message.register(
            self.__licence_handler, F.text == "/licence"
        )
        self.router.message.register(self.__end_handler, F.text == "/end")
        self.router.message.register(
            self.__add_command_handler, F.text == "/add"
        )
        self.router.message.register(
            self.__remove_command_handler, F.text == "/remove"
        )
        self.router.message.register(
            self.__get_channels, F.text == "/get_channels")
        self.router.message.register(
            self.__handle_source, AddSourceStates.waiting_for_source
        )
        self.router.message.register(
            self.__cancel_handler, F.text == "Отмена🔴"
        )
        self.router.message.register(self.__message_handler)

        # --- Хэндлеры для инлайн-кнопок, коллбэки ---
        self.router.callback_query.register(self.__inline_button_handler)

    async def __start_handler(self, message: Message):
        await self.telegram_ui_logger.info(
            f"User {message.from_user.id} started the bot.")

        await self.bot.set_my_commands([
            BotCommand(command="/start", description="Начать работу с ботом"),
            BotCommand(command="/add", description="Добавить источник"),
            BotCommand(command="/remove", description="Удалить источник"),
            BotCommand(command="/end", description="Удалить аккаунт"),
            BotCommand(command="/licence", description="Информация о лицензии")
        ])

        await message.answer(
            f"Добро пожаловать, {message.from_user.first_name}!\n\n"
            "<u>Доступные команды:</u>\n\n"
            "/add — для добавления источника,\n"
            "/remove — для удаления \n"
            "/end — чтобы удалить свой аккаунт.\n\n"
            "Для получения информации о лицензии используйте /licence.",
            reply_markup=ReplyKeyboardRemove()
        )

        try:
            await self.DataBaseHelper.create_user(
                message.from_user.id,
                message.from_user.first_name
            )
        except ValueError:
            pass

    @staticmethod
    async def __licence_handler(message: Message):
        await message.answer(
            "Проект находится под лицензией AGPL v3:\n"
            "https://www.gnu.org/licenses/agpl-3.0.txt"
        )

    async def __end_handler(self, message: Message):
        await message.answer(
            "Вы успешно вышли из сервиса. Все данные будут удалены.",
            reply_markup=ReplyKeyboardRemove()
        )
        channels = await self.DataBaseHelper.delete_user(message.from_user.id)

        for channel in channels:
            await self.Scrapper.unsubscribe_from_channel(channel)

    @staticmethod
    async def __add_command_handler(
        message: Message, state: FSMContext
    ):
        cancel_button = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отмена🔴")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await message.answer(
            "Введите ссылку на источник или нажмите 'Отмена🔴':",
            reply_markup=cancel_button
        )
        await state.set_state(AddSourceStates.waiting_for_source)

    @staticmethod
    async def __cancel_handler(message: Message, state: FSMContext):
        await state.clear()
        await message.answer(
            "Добавление источника отменено.",
            reply_markup=ReplyKeyboardRemove()
        )

    async def __handle_source(self, message: Message, state: FSMContext):
        if message.text == "Отмена🔴":
            await self.__cancel_handler(message, state)
            return

        source_link = message.text

        channel_info = await self.Scrapper.subscribe_to_channel(
            source_link
        )

        if (channel_info["status"] == "success" or
                channel_info["status"] == "already_subscribed"):

            try:
                await self.DataBaseHelper.create_channel(
                    channel_info["channel_id"],
                    channel_info["channel_name"]
                )
            except ValueError:
                pass

            await self.DataBaseHelper.update_user_channels(
                message.from_user.id,
                add=[int(channel_info["channel_id"])]
            )
        elif channel_info["status"] == "private_channel":
            await message.answer(
                "Приватные каналы пока не поддерживаются."
            )
            await state.clear()
            return
        elif channel_info["status"] == "error":
            await message.answer(
                "Ошибка при добавлении источника. "
                "Пожалуйста, проверьте ссылку и попробуйте снова."
            )
            await state.clear()
            return
        else:
            await message.answer(
                "Неизвестная ошибка. Пожалуйста, попробуйте позже."
            )
            await state.clear()
            return

        await message.answer(
            f"Источник \"{channel_info['channel_name']}\" добавлен!",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.clear()

    async def __get_channels(self, message: Message):
        try:
            user = await self.DataBaseHelper.get_user(message.from_user.id)
        except ValueError:
            await message.answer(
                "Вы не зарегистрированы в системе. Добавьте хотя бы один"
                " источник, чтобы получить доступ к этой функции."
            )
            return None

        user_channels = user.channels
        channel_names = []
        for channel in user_channels:
            chat = await self.bot.get_chat(channel)
            if chat:
                channel_names.append(f"id: {channel}, Имя: {chat.title}")
            else:
                channel_names.append(f"id: {channel}, Имя: Неизвестный канал")
        await message.answer(
            "Ваши источники:\n" + "\n".join(channel_names),
            reply_markup=ReplyKeyboardRemove()
        )
        return None

    async def __get_channels_internal(self, user_id: int):
        try:
            user = await self.DataBaseHelper.get_user(user_id)
        except ValueError:
            return None

        user_channels = user.channels
        channel_names = []
        for channel in user_channels:
            info = await self.DataBaseHelper.get_channel(channel)
            if info:
                channel_names.append({"id": info.id, "name": info.name})
            else:
                channel_names.append(
                    {"id": info.id, "name": "Неизвестный канал"})

        return channel_names

    async def __remove_command_handler(self, message: Message):
        channels = await self.__get_channels_internal(message.from_user.id)
        if not channels:
            await message.answer(
                "У вас нет добавленных источников. Пожалуйста, добавьте хотя бы один источник."
            )
            return
        await self.__send_paginated_channels(message, channels, page=1)

    async def _response_loop(self):
        while True:
            response = await self.RagClient.response_queue.get()
            if response is None:
                continue
            await self.bot.send_message(
                response["user_id"],
                response["response_text"],
            )
            print(f"Got response: {response}")

    @staticmethod
    async def __send_paginated_channels(
        message: Message,
        channels,
        page: int
    ):
        items_per_page = 5
        start = (page - 1) * items_per_page
        end = start + items_per_page
        current_page_channels = channels[start:end]

        inline_keyboard = [
            [
                InlineKeyboardButton(
                    text=channel["name"],
                    callback_data=f"usr:{message.from_user.id} rm:{channel['id']}"
                )
            ]
            for channel in current_page_channels
        ]

        navigation_buttons = []
        if page > 1:
            navigation_buttons.append(InlineKeyboardButton(
                text="<<<", callback_data=f"page:{page - 1}"))
        if end < len(channels):
            navigation_buttons.append(InlineKeyboardButton(
                text=">>>", callback_data=f"page:{page + 1}"))
        if navigation_buttons:
            inline_keyboard.append(navigation_buttons)

        markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

        try:
            await message.edit_text(
                "Выберите канал для удаления:",
                reply_markup=markup
            )
        except Exception:
            await message.delete()
            await message.answer(
                "Выберите канал для удаления:",
                reply_markup=markup
            )

    async def __inline_button_handler(self, callback_query: CallbackQuery):
        callback_data = callback_query.data
        if callback_data.startswith("usr:"):
            usr_str, channel_str = callback_data.split(" ")
            user_id = int(usr_str.split(":")[1])
            channel_id = int(channel_str.split(":")[1])
            try:
                channels = await self.DataBaseHelper.update_user_channels(
                    user_id,
                    remove=[channel_id]
                )
                for channel in channels:
                    print(channel)
                    print(type(channel))
                    await self.Scrapper.unsubscribe_from_channel(
                        channel
                    )
                await callback_query.message.edit_text(
                    f"Канал с ID {channel_id} удален из отслеживаемых."
                )
            except ValueError:
                await callback_query.message.edit_text(
                    f"Канал с ID {channel_id} не найден."
                )
        elif callback_data.startswith("page:"):
            page = int(callback_data.split(":")[1])
            channels = await self.__get_channels_internal(
                user_id=callback_query.from_user.id
            )
            await self.__send_paginated_channels(
                callback_query.message,
                channels,
                page
            )
        await callback_query.answer()

    async def __message_handler(self, message: Message):
        if not message.text:
            await message.answer(
                "Пожалуйста, отправьте текстовое сообщение."
                " Стикеры, голосовые и другие типы"
                " сообщений не поддерживаются."
            )
            return
        if message.from_user.id == self.bot.id:
            await message.answer(
                "Черезвычайно извиняюсь, но я не могу"
                " обрабатывать сообщения от себя."
            )
            return

        try:
            user = await self.DataBaseHelper.get_user(message.from_user.id)
        except ValueError:
            await self.telegram_ui_logger.error("Could not get user from DB.")
            await message.answer(
                "Вы не зарегистрированы в системе. Пожалуйста,"
                " добавьте источник, чтобы получить доступ к этой функции."
            )
            return

        user_channels: list[int] = user.channels
        if not user_channels:
            await self.telegram_ui_logger.error(
                "User has no channels. Or there is something wrong with DB."
            )
            await message.answer(
                "У вас нет добавленных источников."
                " Пожалуйста, добавьте хотя бы один источник."
            )
            return

        await message.answer(
            "Сообщение получено! Ожидайте ответа RAG."
        )

        texts = []
        for channel in user_channels:
            channel_name = await self.DataBaseHelper.get_channel(channel)
            posts = await self.Scrapper.fetch(channel)
            texts.append(
                {
                    "channel_id": channel,
                    "channel_name": channel_name.name,
                    "posts": posts
                }
            )

        self.RagClient.request_queue.put_nowait(
            {
                "user_id": message.from_user.id,
                "request_text": message.text,
                "texts": texts
            }
        )

    async def start(self):
        self._response_task = asyncio.create_task(self._response_loop())
        await self.dispatcher.start_polling(self.bot)

    async def stop(self):
        if self._response_task:
            self._response_task.cancel()
            try:
                await self._response_task
            except asyncio.CancelledError:
                pass
        await self.bot.session.close()
