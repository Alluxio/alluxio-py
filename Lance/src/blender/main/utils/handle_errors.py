import functools
import logging
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    )
    logger.addHandler(ch)


def handle_errors(
    action: str,
) -> Callable[[Callable[..., Dict[str, Any]]], Callable[..., Dict[str, Any]]]:
    def decorator(
        func: Callable[..., Dict[str, Any]]
    ) -> Callable[..., Dict[str, Any]]:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs) -> Dict[str, Any]:
            cls = self.__class__.__name__
            target: Optional[str]
            if action == "download":
                target = getattr(self, "name", "<unknown>")
            else:
                target = args[0] if args else "<unknown>"
            logger.info(f"[START]   {cls}.{action}('{target}')")
            try:
                result = func(self, *args, **kwargs)
                logger.info(f"[SUCCESS] {cls}.{action}('{target}')")
                return result
            except Exception as e:
                logger.error(
                    f"[FAILURE] {cls}.{action}('{target}') failed: {e}",
                    exc_info=True,
                )
                raise

        return wrapper

    return decorator
