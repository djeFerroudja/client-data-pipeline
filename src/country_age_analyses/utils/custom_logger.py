import logging
import colorlog

# Définir un niveau SUCCESS personnalisé
SUCCESS_LEVEL_NUM = 25
logging.addLevelName(SUCCESS_LEVEL_NUM, "SUCCESS")


def success(self, message, *args, **kwargs):
    """
    Log a message with level SUCCESS (between INFO and WARNING).

    Args:
        message (str): The message to log.
        *args: Additional positional arguments passed to the logger.
        **kwargs: Additional keyword arguments passed to the logger.
    """
    if self.isEnabledFor(SUCCESS_LEVEL_NUM):
        self._log(SUCCESS_LEVEL_NUM, message, args, **kwargs)


# Ajouter la méthode success à la classe Logger
logging.Logger.success = success


class AlignedColoredFormatter(colorlog.ColoredFormatter):
    """
    Custom ColoredFormatter that aligns log messages to a fixed column
    and adds color based on log level.

    Overrides:
        format(record): Adds padding to align messages consistently.
    """

    def format(self, record):
        """
        Format the log record with padding and colors.

        Args:
            record (logging.LogRecord): Log record to format.

        Returns:
            str: Formatted log message.
        """
        max_level_len = 12  # Length of the longest log level (CRITICAL)
        levelname = record.levelname
        padding = max_level_len - len(levelname)
        # Add padding before the message, 2 spaces after padding
        record.msg = " " * padding + "  " + str(record.msg)
        return super().format(record)


# Définition du formatter avec couleurs et date
formatter = AlignedColoredFormatter(
    "%(log_color)s[%(asctime)s]\t[%(levelname)s]%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    log_colors={
        "DEBUG": "cyan",
        "INFO": "bold_white",
        "SUCCESS": "bold_green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "bold_red",
    },
)

# Handler pour afficher les logs sur la console
handler = colorlog.StreamHandler()
handler.setFormatter(formatter)

# Logger personnalisé réutilisable dans le projet
CustomLogger = colorlog.getLogger(__name__)
CustomLogger.addHandler(handler)
CustomLogger.setLevel(logging.INFO)
