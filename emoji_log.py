import logging


EMOJIS = {
    # General status
    "start": "\U0001F680",       # 🚀
    "success": "\u2705",         # ✅
    "fail": "\u274C",            # ❌
    "error": "\U0001F6A8",       # 🚨
    "warning": "\u26A0",         # ⚠️
    "info": "\u2139",            # ℹ️
    "stop": "\u23F9",            # ⏹️

    # Progress
    "loading": "\U0001F504",     # 🔄
    "in_progress": "\u23F3",     # ⏳
    "done": "\u2705",            # ✅

    # Data / I/O
    "download": "\U0001F4E5",    # 📥
    "upload": "\U0001F4E4",      # 📤
    "database": "\U0001F4BE",    # 💾
    "file": "\U0001F4C4",        # 📄
    "folder": "\U0001F4C2",      # 📂

    # Dev / Code / System
    "debug": "\U0001F41B",       # 🐛
    "deploy": "\U0001F4BB",      # 💻
    "config": "\U00002699",      # ⚙️
    "lock": "\U0001F512",        # 🔒

    # Metrics / Charts
    "chart": "\U0001F4C8",       # 📈
    "summary": "\U0001F4CA",     # 📊
    "report": "\U0001F4CB",      # 📋

    # Communication
    "mail": "\U0001F4E7",        # 📧
    "comment": "\U0001F4AC",     # 💬
    "link": "\U0001F517",        # 🔗

    # Misc
    "sparkle": "\u2728",         # ✨
    "checklist": "\U0001F4DD",   # 📝
    "lightbulb": "\U0001F4A1",   # 💡
    "poop": "\U0001F4A9",        # 💩
    "heart": "\U00002764",       # ❤️
    "thumbs_up": "\U0001F44D",   # 👍
    "thumbs_down": "\U0001F44E",  # 👎
    "fish": "\U0001F41F",        # 🐟
    "water": "\U0001F4A7",       # 💧
    "fishing": "\U0001F3A3",     # 🎣
    "database": "\U0001F4BE",    # 💾
}


def log_with_emoji(level: str, msg: str, emoji_name: str) -> None:
    """
    Log a message with an emoji prefix based on the log level.

    Parameters:
        level (str): The logging level (e.g., 'info', 'warning', 'error').
        msg (str): The message to log.
        emoji_name (str): The name of the emoji to use as a prefix.
    """
    emoji_icon = EMOJIS.get(emoji_name, "")
    full_msg = f"{emoji_icon} {msg}"
    getattr(logging, level)(full_msg)
