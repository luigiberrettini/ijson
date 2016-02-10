from . import test_asyncio


class AsyncForParse(test_asyncio.AsyncioParse):
    """
    This is just a helper to make sure all methods are also compatibly with 3.5 async for syntax
    """
    async def list(self, coro):
        items = []
        async for item in coro:
            items.append(item)
        return items
