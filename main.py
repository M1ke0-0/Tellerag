from source import get_config, TeleRagService
import asyncio


async def main():
    """
    Main function to start the TeleRagService.
    """
    settings = get_config()
    service = TeleRagService(settings)
    await service.start()
    await service.idle()

if __name__ == '__main__':
    asyncio.run(main())
