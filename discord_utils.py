import os
import logging
import asyncio
import discord
from discord.ext import commands
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("discord_utils")

# Load environment variables
load_dotenv()

# Get Discord token from environment variables
DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DEFAULT_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    """Event handler for when the bot is ready and connected to Discord."""
    logger.info(f'Bot connected as {bot.user.name} (ID: {bot.user.id})')
    logger.info(f'Bot is in {len(bot.guilds)} guilds')

    for guild in bot.guilds:
        logger.info(f'- {guild.name} (ID: {guild.id})')

    # Set bot status
    await bot.change_presence(activity=discord.Game(name="bot is ready"))

@bot.command(name="status")
async def status(ctx):
    """Command to check the bot status."""
    await ctx.send(f"Spark Pipeline Debugger is online! Monitoring for failures.")

# use this to connect to spark
async def send_discord_message(message: str, channel_id: int = None):
    """
    Send a message to a specific Discord channel.

    Args:
        message (str): The message to send
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

        # Send the message
        await channel.send(message)
        logger.info(f"Message sent to Discord channel {channel_id}")
        return True

    except Exception as e:
        logger.error(f"Error sending message to Discord: {str(e)}")
        return False

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
            logger.info(f"Failure alert for {failure_type} sent to Discord")
            return True
        else:
            logger.error(f"Could not find Discord channel with ID {channel_id}")
            return False
    except Exception as e:
        logger.error(f"Error sending failure alert to Discord: {str(e)}")
        return False

def start_discord_bot():
    """
    Start the Discord bot in a non-blocking way.
    This function should be called when the server starts.
    """
    if not DISCORD_TOKEN:
        logger.error("Discord bot token not found. Make sure to set DISCORD_BOT_TOKEN in .env")
        return False

    # Run the bot in a separate thread
    def run_bot_thread():
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
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

    logger.info("Discord bot started in background thread")
    return True