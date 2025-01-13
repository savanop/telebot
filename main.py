from telethon import TelegramClient, events, sync
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel, User
from datetime import datetime, timedelta
import asyncio
import time
import pytz
from collections import defaultdict

# API credentials
api_id = api_id
api_hash = 'api hash'
phone = '+91number'

# Target and output group links
TARGET_GROUP = 'https://t.me/ktrlooters'
OUTPUT_GROUP = 'https://t.me/hxhd72'

# Constants for rate limiting
MESSAGES_PER_BATCH = 200  # Number of messages to process before cooldown
COOLDOWN_TIME = 60  # Cooldown time in seconds
MAX_RETRIES = 3  # Maximum number of retries for failed operations

# Initialize client with larger timeout
client = TelegramClient('session_name', api_id, api_hash, 
                       connection_retries=MAX_RETRIES,
                       retry_delay=1)

async def get_group_entity(group_link):
    for _ in range(MAX_RETRIES):
        try:
            return await client.get_entity(group_link)
        except Exception as e:
            print(f"Error getting group entity: {e}")
            await asyncio.sleep(COOLDOWN_TIME)
    raise Exception("Failed to get group entity after maximum retries")

async def process_message_batch(messages, user_stats, start_time):
    processed = 0
    for message in messages:
        try:
            if message.from_id:
                sender = await client.get_entity(message.from_id)
                if isinstance(sender, User):
                    user_stats[sender.id]['messages'] += 1
                    user_stats[sender.id]['reads'] += message.views or 0
                    user_stats[sender.id]['last_active'] = message.date
                    
                    # Enhanced message details logging
                    print(f"\nNew Message Read:")
                    print(f"From: {sender.first_name} {sender.last_name or ''} (@{sender.username or 'No username'})")
                    print(f"Message: {message.message[:100]}..." if len(message.message) > 100 else f"Message: {message.message}")
                    print(f"Views: {message.views or 0}")
                    print(f"Date: {message.date}")
                    print(f"Message ID: {message.id}")
                    if message.media:
                        print(f"Media Type: {type(message.media).__name__}")
                    print("-" * 50)
                    
                    # Enhanced user level calculation based on messages and views
                    msg_count = user_stats[sender.id]['messages']
                    view_count = user_stats[sender.id]['reads']
                    
                    if msg_count > 200 and view_count > 1000:
                        user_stats[sender.id]['level'] = 6  # VIP level
                    elif msg_count > 100:
                        user_stats[sender.id]['level'] = 5
                    elif msg_count > 50:
                        user_stats[sender.id]['level'] = 4
                    elif msg_count > 25:
                        user_stats[sender.id]['level'] = 3
                    elif msg_count > 10:
                        user_stats[sender.id]['level'] = 2
                    else:
                        user_stats[sender.id]['level'] = 1
                        
            processed += 1
            
            # Progress tracking
            if processed % 20 == 0:  # Show progress more frequently
                elapsed_time = time.time() - start_time
                print(f"Processed {processed} messages in current batch")
                print(f"Time elapsed: {elapsed_time:.2f} seconds")
                
        except Exception as e:
            print(f"Error processing message: {e}")
            await asyncio.sleep(1)  # Brief pause on error
            
    return processed

async def get_messages_and_stats():
    target_group = await get_group_entity(TARGET_GROUP)
    output_group = await get_group_entity(OUTPUT_GROUP)
    
    tz = pytz.timezone('Asia/Kolkata')
    now = datetime.now(tz)
    week_ago = now - timedelta(days=7)
    
    user_stats = defaultdict(lambda: {
        'messages': 0, 
        'reads': 0, 
        'level': 0,
        'last_active': None
    })
    
    start_time = time.time()
    
    try:
        offset_id = 0
        total_processed = 0
        batch_count = 0
        
        while True:
            try:
                messages = await client(GetHistoryRequest(
                    peer=target_group,
                    limit=MESSAGES_PER_BATCH,
                    offset_date=None,
                    offset_id=offset_id,
                    max_id=0,
                    min_id=0,
                    add_offset=0,
                    hash=0
                ))
                
                if not messages.messages:
                    break
                    
                # Process messages in current batch
                processed = await process_message_batch(messages.messages, user_stats, start_time)
                total_processed += processed
                batch_count += 1
                
                # Update offset for next batch
                offset_id = messages.messages[-1].id
                
                # Check if we've reached messages older than a week
                if messages.messages[-1].date < week_ago:
                    break
                
                print(f"\nBatch {batch_count} completed:")
                print(f"Total messages processed: {total_processed}")
                print(f"Taking cooldown period of {COOLDOWN_TIME} seconds...")
                await asyncio.sleep(COOLDOWN_TIME)
                
            except Exception as e:
                print(f"Error in batch processing: {e}")
                await asyncio.sleep(COOLDOWN_TIME * 2)  # Double cooldown on error
        
        # Enhanced report generation
        report = "📊 Weekly Activity Report 📊\n\n"
        report += f"Period: {week_ago.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}\n"
        report += f"Total Messages Processed: {total_processed}\n\n"
        
        # Sort users by message count
        sorted_users = sorted(user_stats.items(), 
                            key=lambda x: x[1]['messages'], 
                            reverse=True)
        
        for user_id, stats in sorted_users:
            try:
                user = await client.get_entity(user_id)
                username = user.username or f"{user.first_name} {user.last_name or ''}"
                report += f"👤 User: {username}\n"
                report += f"📝 Messages: {stats['messages']}\n"
                report += f"👁 Total Views: {stats['reads']}\n"
                report += f"⭐ Level: {stats['level']}"
                if stats['level'] == 6:
                    report += " (VIP)"
                report += f"\n🕒 Last Active: {stats['last_active'].strftime('%Y-%m-%d %H:%M')}\n"
                report += "─" * 30 + "\n"
            except Exception as e:
                print(f"Error getting user info: {e}")
                continue
        
        # Send report in chunks if too long
        report_chunks = [report[i:i+4096] for i in range(0, len(report), 4096)]
        for chunk in report_chunks:
            await client.send_message(output_group, chunk)
            await asyncio.sleep(2)  # Brief pause between chunks
        
        print("Report generated and sent successfully!")
        print(f"Total processing time: {time.time() - start_time:.2f} seconds")
        
    except Exception as e:
        print(f"An error occurred: {e}")

async def main():
    print("Starting Telegram Analytics Tool...")
    await client.start(phone)
    print("Client initialized successfully!")
    
    while True:
        try:
            await get_messages_and_stats()
            print("Waiting 24 hours before next update...")
            await asyncio.sleep(24 * 60 * 60)
        except Exception as e:
            print(f"Error in main loop: {e}")
            await asyncio.sleep(COOLDOWN_TIME)

if __name__ == '__main__':
    client.loop.run_until_complete(main())
