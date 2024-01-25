from telethon.sync import TelegramClient, events
import sqlite3
from datetime import datetime
import schedule, time, socket
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import config, re

api_id = 11111
api_hash = '2222221111'
bot_token = 'xxx:xxxxxxx' #Bot TTW

B_TTW = 6640331247 

TTW_NOTIFY = config.TTW_NOTIFY 
CT_NOTIFY = config.CT_NOTIFY 

min_vol = config.min_vol
min_vol_a = config.min_vol_a
min_vol_au = config.min_vol_au
min_vol_my = config.min_vol_my

min_liqui = config.min_liqui
min_delta = config.min_delta

CT_NOTIFY_STOPRUN = config.CT_NOTIFY_STOPRUN
CT_NOTIFY_VOLUMESTOP = config.CT_NOTIFY_VOLUMESTOP
CT_NOTIFY_HIDDENORDER = config.CT_NOTIFY_HIDDENORDER
CT_NOTIFY_SWEEP = config.CT_NOTIFY_SWEEP
CT_NOTIFY_ICEBERG = config.CT_NOTIFY_ICEBERG
CT_NOTIFY_LIQUIDITY = config.CT_NOTIFY_LIQUIDITY 
CT_NOTIFY_IMBALANCE = config.CT_NOTIFY_IMBALANCE
CT_NOTIFY_VOLUMESPIKE = config.CT_NOTIFY_VOLUMESPIKE
CT_NOTIFY_ABSORPTION = config.CT_NOTIFY_ABSORPTION
CT_NOTIFY_PRICE = config.CT_NOTIFY_PRICE
CT_NOTIFY_VDELTA = config.CT_NOTIFY_VDELTA
CT_NOTIFY_MARKETVOLUME = config.CT_NOTIFY_MARKETVOLUME
CT_NOTIFY_TREND = config.CT_NOTIFY_TREND

CT_ALL_MIN = config.CT_ALL_MIN # all events >=10
CT_Events_MIN = config.CT_Events_MIN # 5 events >=20
CT_FLOW = config.CT_FLOW # 5 events với từng config của events riêng

CT_ALL_MIN_Vol = config.CT_ALL_MIN_Vol 
CT_Events_MIN_Vol = config.CT_Events_MIN_Vol 
#CT_FLOW_Vol = config.CT_FLOW_Vol 
C_Hiden = config.C_Hiden
C_Stop = config.C_Stop
C_Trend = config.C_Trend
C_Sweep = config.C_Sweep
C_Absorption = config.C_Absorption

client = TelegramClient('ttw', api_id, api_hash)
computer_name = socket.gethostname()

def get_topic_tosend(text):
    if 'ttw stop run' in text.lower() or 'ttw volume stop' in text.lower():
        min_vol = C_Stop
        return CT_NOTIFY_STOPRUN
    elif 'ttw market volume' in text.lower():
        return CT_NOTIFY_MARKETVOLUME
    elif 'ttw hidden order' in text.lower() or 'iceberg' in text.lower():
        min_vol = C_Hiden
        return CT_NOTIFY_HIDDENORDER
    elif 'ttw sweep' in text.lower():
        min_vol = C_Sweep
        return CT_NOTIFY_SWEEP
    elif 'ttw liquidity' in text.lower():
        return CT_NOTIFY_LIQUIDITY
    elif 'ttw imbalance' in text.lower():
        return CT_NOTIFY_IMBALANCE
    elif 'volumespike' in text.lower():
        return CT_NOTIFY_VOLUMESPIKE
    elif 'ttw absorption' in text.lower():
        min_vol = C_Absorption
        return CT_NOTIFY_ABSORPTION
    elif 'ttw price alert' in text.lower():
        return CT_NOTIFY_PRICE
    elif 'ttw v-delta' in text.lower():
        return CT_NOTIFY_VDELTA
    elif 'ttw trend' in text.lower():
        return CT_NOTIFY_TREND

def get_noti_vol(lines):
    vol = 0
    for line in lines:
        if 'volume' in line:
            vol = float(line.split(":")[1].strip())
    return vol

def storage_datapoint(lines):
    tag = "" 
    vol = 0 
    price = 0
    ins = ""
    liqui = 0
    delta = 0
    pricechange = 0
    datapoint = re.sub(r'[^a-zA-Z\s]', '', lines[0]).replace("ttw","").strip().upper()
    for line in lines:
        if 'side' in line: #BID/ASK
            tag = line.split(":")[1].strip()
        elif 'price' in line:
            price = float(line.split(":")[1].strip())
        elif 'volume' in line:
            vol = float(line.split(":")[1].strip())
        elif 'instrument' in line:
            ins = line.split(":")[1].strip().split(".")[0]
        elif 'delta' in line:
            delta = float(line.split(":")[1].strip())
        elif 'liquidity' in line:
            liqui = float(re.sub(r'[^0-9]', '', line.split(":")[1].strip()))
        elif 'price change' in line:
            pricechange = float(line.split(":")[1].strip())
    point = (
                Point(datapoint)
                .tag("Side", tag)
                .field("Price", price).field("Vol", vol).field("PriceChange", pricechange).field("Delta", delta)
                .time(datetime.utcnow())
            )
    write_influx(point)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    write_sqlite(ins,dt_string,datapoint,tag,price,vol,pricechange)
    
@client.on(events.NewMessage)
async def mess_handler(event):
    chat_id = event.chat_id
    text:str = event.raw_text
    # dd/mm/YY H:M:S
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    
    
    # check mes from group TTW
    if chat_id == B_TTW:
        mess = "{0}\n{1}".format(dt_string,event.raw_text)
        lines = text.lower().split("\n")
        topic = get_topic_tosend(lines[0])
        noti_vol = get_noti_vol(lines)
        # send all >=10
        if noti_vol >= CT_ALL_MIN_Vol:
            await client.send_message(entity=CT_NOTIFY,message=mess,reply_to=CT_ALL_MIN)
        # send events (stop,hden....) >=20
        if noti_vol >= CT_Events_MIN_Vol:
            await client.send_message(entity=CT_NOTIFY,message=mess,reply_to=CT_Events_MIN)
        # send flow by config
        if noti_vol >= min_vol or topic == CT_NOTIFY_TREND:
            await client.send_message(entity=CT_NOTIFY,message=mess,reply_to=topic)
            await client.send_message(entity=CT_NOTIFY,message=mess,reply_to=CT_FLOW)
        storage_datapoint(lines)
        print("{0}\nMessage: '{1}' \nFrom chatID: '{2}' \nSend to group {3} topic {4}".format(dt_string,event.raw_text,chat_id,CT_NOTIFY,topic))
    

# Insert database sqlite

def write_sqlite(Instrument,date,Event,Side,Price,Volume,PriceChange):
    try:
        conn=sqlite3.connect("ttw.db")
        cursor=conn.cursor()
        cursor.execute("INSERT INTO ttw (Instrument,Date,Event,Side,Price,Volume,PriceChange) VALUES (?,?,?,?,?,?,?)",(Instrument,date,Event,Side,Price,Volume,PriceChange))
        conn.commit()
        conn.close()
    except Exception as e:
        client.send_message(entity=CT_NOTIFY,message="Can not insert into sqlite3!!!",reply_to=1)


def write_influx(point):
    try:
        token = "YyWbGAC2KN-JWx-NOkMrAtVwWpXY_7KfOpL3tyuWVaL6UK5f8JbRnee9knG7ZFw8tdRwgSkw1VK5HsfBfczyug=="
        org = "ttw"
        url = "http://10.10.13.103:8086"
        bucket="ttw"
        clientDB = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
        write_api = clientDB.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=bucket, org="ttw", record=point)
    except Exception as e:
        client.send_message(entity=CT_NOTIFY,message="Can not insert into InfluxDB!!!",reply_to=1)
#    
def checkrunning(mess):
    client.send_message(entity=CT_NOTIFY,message= mess,reply_to=1)

def schedule_messages():
    schedule.every().hour.at("09:00").do(checkrunning,mess=f"Bot is running on {computer_name}")
    while True:
        schedule.run_pending() 
        time.sleep(1)  

@client.on(events.NewMessage(pattern='/status'))
async def status(event):
    #await client.send_message(entity=CT_NOTIFY,message=f"I'm running on {computer_name}",reply_to=1)
    await event.respond(f"I'm running on {computer_name}")

@client.on(events.NewMessage(pattern='/config'))
async def config(event):
    #await client.send_message(entity=CT_NOTIFY,message=f"I'm running on {computer_name}",reply_to=1)
    await event.respond(f"give me configname")
    text:str = event.raw_text.lower()
    if 'stop run' in text:
        await event.respond(f"Config is: {config.C_Stop}")
    elif 'hiden order' in text:
        await event.respond(f"Config is: {config.C_Hiden}")
    elif 'absorption' in text:
        await event.respond(f"Config is: {config.C_Absorption}")
    elif 'sweep' in text:
        await event.respond(f"Config is: {config.C_Sweep}")

@client.on(events.NewMessage(pattern='/video'))
async def videocall(event):
    await event.respond('May i help you?')
    #await client.send_message(entity=CT_NOTIFY,message=f"Started video call on BM15 and BM1H",reply_to=1)
#set lich
@client.on(events.NewMessage(pattern='/schedule'))
async def schedule(event):
    await event.respond('Hello! Welcome to the bot. May i help you?')
    await client.send_message(entity=CT_NOTIFY,message=f"I'm running on {computer_name}",reply_to=1)

client.start()
print("bot is ready.")
client.send_message(entity=CT_NOTIFY,message=f"Bot is start on {computer_name}")
client.run_until_disconnected()