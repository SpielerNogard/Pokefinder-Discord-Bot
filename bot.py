import os
from dotenv import load_dotenv
from pokefinder.logging.get_logger import get_logger
from pokefinder.mqtt.mqtt_sender import MQTTSender
from pokefinder.mqtt.mqtt_broker import MQTTBroker
from pokefinder.health.health_checker import start_health_check
from threading import Thread
import discord
from discord.ext import tasks, commands
from discord import Embed
load_dotenv()


bot = discord.Bot()
sender = MQTTSender('messages/in')
broker = MQTTBroker('messages/out')
logger = get_logger('INFO')
token = os.getenv('bot_token')
logger.info(token)

def create_answer_broker():
    broker.run()

def start_broker():
    message_in = Thread(name="PokemonBroker", target=create_answer_broker)
    message_in.daemon = True
    message_in.start()

async def set_status(status, presence=discord.Status.idle):
    logger.info(f'setting status: {status}')
    game = discord.Game(status)
    await bot.change_presence(status=presence, activity=game)

async def get_user(user_id):
    return (await bot.get_or_fetch_user(user_id))

async def get_channel(channel_id):
    return bot.get_channel(channel_id)

async def send_message(receiver, content=None, embed=None):
    await receiver.send(content=content, embed=embed)

@bot.listen()
async def on_message(message):
    channel = message.channel.id
    message_id = message.id
    message_content = message.content
    user_id = message.author.id
    message_info={'channel':channel,
                'message_id':message_id,
                'content':message_content,
                'user':user_id}
    sender.send_message(message_info)
    logger.info(message)


@bot.event
async def on_ready():
    start_broker()
    start_health_check('discord-bot')
    logger.info(f"We have logged in as {bot.user}")
    await set_status('with the API')
    message_in.start()

@tasks.loop(seconds=1)
async def message_in():
    logger.info('checking messages')
    message = broker.next_message()
    if message is None:
        logger.info('no new messages received')
    else:
        if message.get('user') is not None:
            receiver = await get_user(message.get('user'))
        elif message.get('channel') is not None:
            receiver = await get_channel(message.get('channel'))

        if message.get('text') is not None:
            await send_message(receiver,content=message.get('text'))
        elif message.get('embed') is not None:
            await send_message(receiver, embed=Embed.from_dict(message.get('embed')))
        elif message.get('status') is not None:
            await set_status(message.get('status'))

bot.run(token)
