import os
import logging
import asyncio
import discord
from discord.ext import commands
from dotenv import load_dotenv
import io
from typing import Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Load environment variables
load_dotenv()

# Get Discord token from environment variables (expects DISCORD_TOKEN)
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
if DISCORD_TOKEN is not None:
    DISCORD_TOKEN = DISCORD_TOKEN.strip()
DEFAULT_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Global variable to store the bot's event loop
bot_loop = None

@bot.event
async def on_ready():
    """Event handler for when the bot is ready and connected to Discord."""
    # Set bot status
    await bot.change_presence(activity=discord.Game(name="bot is ready"))

@bot.command(name="status")
async def status(ctx):
    """Command to check the bot status."""
    await ctx.send(f"Spark Pipeline Debugger is online! Monitoring for failures.")

def run_in_bot_loop(coro):
    """
    Run a coroutine in the bot's event loop from any thread.
    """
    global bot_loop
    if bot_loop and bot_loop.is_running():
        import concurrent.futures
        future = asyncio.run_coroutine_threadsafe(coro, bot_loop)
        try:
            return future.result()  # Wait for result and propagate exceptions
        except concurrent.futures.TimeoutError:
            logger.error("Timed out waiting for Discord coroutine to finish.")
            return False
        except Exception as e:
            logger.error(f"Exception in Discord coroutine: {e}")
            return False
    else:
        logger.error("Bot event loop is not running.")
        return False

async def send_long_message(channel: discord.TextChannel, content: str):
    """
    Handles sending long messages to Discord by either:
    1. Using an embed for messages up to 6000 chars
    2. Splitting into multiple messages for medium length content
    3. Uploading as a file for very long content
    
    Args:
        channel: The Discord channel to send to
        content: The message content
    """
    if len(content) <= 2000:
        # Regular message
        await channel.send(content)
    elif len(content) <= 6000:
        # Use embed for longer messages
        embed = discord.Embed(
            description=content,
            color=discord.Color.blue()
        )
        await channel.send(embed=embed)
    elif len(content) <= 10000:
        # Split into multiple messages
        while content:
            # Find last newline before 2000 chars if possible
            split_point = content[:2000].rfind('\n')
            if split_point == -1:
                split_point = 2000
            
            await channel.send(content[:split_point])
            content = content[split_point:].lstrip()
    else:
        # Send as file for very long content
        file = discord.File(
            io.StringIO(content),
            filename="report.txt"
        )
        await channel.send("Report (see attached file):", file=file)

async def send_discord_message(message: Union[str, discord.Embed], channel_id: int = None):
    """
    Send a message to a specific Discord channel.

    Args:
        message (Union[str, discord.Embed]): The message to send or an embed
        channel_id (int): The ID of the channel to send the message to
    """
    if not DISCORD_TOKEN:
        logger.error("Discord bot token not found in environment variables")
        return False

    channel_id = channel_id or DEFAULT_CHANNEL_ID
    if not channel_id:
        logger.error("No channel ID provided and no default channel ID found in environment variables")
        return False

    try:
        # Get the channel
        channel = bot.get_channel(int(channel_id))
        if not channel:
            logger.error(f"Could not find channel with ID {channel_id}")
            return False

        # Send the message based on type
        if isinstance(message, discord.Embed):
            await channel.send(embed=message)
        else:
            await send_long_message(channel, str(message))
        return True

    except Exception as e:
        logger.error(f"Error sending message to Discord: {str(e)}")
        return False

def send_discord_message_threadsafe(message: str, channel_id: int = None):
    """
    Thread-safe wrapper for send_discord_message.
    """
    return run_in_bot_loop(send_discord_message(message, channel_id))

async def send_failure_alert(failure_type: str, details: dict, channel_id: int = None):
    """
    Send a formatted failure alert to Discord.

    Args:
        failure_type (str): Type of failure (e.g., "job_failure", "exception")
        details (dict): Failure details dictionary
        channel_id (int): Optional channel ID to override default
    """
    embed = discord.Embed(
        title=f"❌ Spark {failure_type.replace('_', ' ').title()} Detected",
        color=discord.Color.red(),
        description="A failure has been detected in your Spark job."
    )

    # Add fields based on failure type
    if failure_type == "job_failure":
        job = details.get("failed_jobs", [{}])[0] if details.get("failed_jobs") else {}

        embed.add_field(name="Application ID", value=job.get("app_id", "Unknown"), inline=True)
        embed.add_field(name="Job ID", value=job.get("job_id", "Unknown"), inline=True)
        embed.add_field(name="Job Name", value=job.get("name", "Unknown"), inline=True)

        if job.get("failure_reason"):
            embed.add_field(
                name="Failure Reason",
                value=f"```{job.get('failure_reason')[:1000]}```",
                inline=False
            )

    elif failure_type == "exception":
        exception = details.get("exceptions", [{}])[0] if details.get("exceptions") else {}

        embed.add_field(name="Exception Type", value=exception.get("type", "Unknown"), inline=True)

        if exception.get("message"):
            embed.add_field(
                name="Message",
                value=f"```{exception.get('message', '')[:1000]}```",
                inline=False
            )

    # Try to get the channel and send
    try:
        channel_id = channel_id or DEFAULT_CHANNEL_ID
        channel = bot.get_channel(int(channel_id))
        if channel:
            await channel.send(embed=embed)
            return True
        else:
            logger.error(f"Could not find Discord channel with ID {channel_id}")
            return False
    except Exception as e:
        logger.error(f"Error sending failure alert to Discord: {str(e)}")
        return False

def send_failure_alert_threadsafe(failure_type: str, details: dict, channel_id: int = None):
    """
    Thread-safe wrapper for send_failure_alert.
    """
    return run_in_bot_loop(send_failure_alert(failure_type, details, channel_id))

def start_discord_bot():
    """
    Start the Discord bot in a non-blocking way.
    This function should be called when the server starts.
    """
    if not DISCORD_TOKEN:
        logger.error("Discord bot token not found. Make sure to set DISCORD_TOKEN in .env")
        return False

    # Run the bot in a separate thread
    def run_bot_thread():
        global bot_loop
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        bot_loop = loop  # Store the loop globally
        asyncio.set_event_loop(loop)

        try:
            # Start the bot
            loop.run_until_complete(bot.start(DISCORD_TOKEN))
        except Exception as e:
            logger.error(f"Error running Discord bot: {str(e)}")
        finally:
            # Clean up
            loop.run_until_complete(bot.close())
            loop.close()

    # Start bot in a separate thread
    import threading
    bot_thread = threading.Thread(target=run_bot_thread, daemon=True)
    bot_thread.start()

    return True