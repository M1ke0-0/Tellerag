import asyncio
from pyrogram import Client
from source.DynamicConfigurationLoading import get_config


async def main():
    settings = get_config()
    async with Client(
        name="./account",
        workdir=".",
        api_id=settings.PYRO_API_ID,
        api_hash=settings.PYRO_API_HASH,
        app_version="1.0",
        device_model="TELERAG",

    ) as app:
        me = await app.get_me()
        print("Вход выполнен\n\n", "Информация о профиле:\n", me)
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
