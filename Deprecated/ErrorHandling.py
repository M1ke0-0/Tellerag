import asyncio
from typing import Optional
class CoreException(Exception):
    def __init__(
            self,
            where: str,
            what: str,
            summary: str,
            full_message: Optional[str] = None,
            fatal: bool = False
    ):
        self.msg_string = f"Exception at {where}: {what}. {'Summary ' + summary if summary else ''} \n {full_message if full_message else ''}"
        self.fatal = fatal
        super().__init__(self.msg_string)




