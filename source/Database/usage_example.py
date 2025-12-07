import asyncio
from source.Database.DBHelper import DataBaseHelper

#  Временный файл как использовать бдшку


async def main():
    # Создаем подключение к БД
    # Нужные коллекции создаст автоматически
    db = await DataBaseHelper.create()

    # Создать канал
    await db.create_channel(id=10001, name="Mash")
    await db.create_channel(id=10002, name="Moscowach")
    await db.create_channel(id=-10003, name="NegIdTest")

    # Создать пользователя
    await db.create_user(user_id=12345, name="User")

    # Подписываем пользователя на два канала
    await db.update_user_channels(user_id=12345, add=[10001, 10002])

    # Удаляем один канал из подписок
    await db.update_user_channels(user_id=12345, remove=[10001])

    # Получить инфу о пользователе
    user = await db.get_user(12345)
    print("User:", user)

    # Удаляем пользователя. У каналов уменьшется количество подписчиков
    await db.delete_user(12345)

    # Пробуем удалить канал без подписчиков
    await db.delete_channel(10001)  # Успешно

    # Ошибка, если есть подписчики на канал
    await db.delete_channel(10002)


if __name__ == "__main__":
    asyncio.run(main())
