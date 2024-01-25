from telethon.sync import TelegramClient, events
import sqlite3
from datetime import datetime
import schedule, time, socket
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

api_id = 24720236
api_hash = '11081bcb0ba19ed7f18927a32a657cd9'
bot_token = '6640331247:AAElbehjFe_9sncuatl13QM7lXVTC1KWEhM' #Bot TTW





B_TTW = 6640331247 #6389158070 #6640331247

TTW_NOTIFY = -1001965745418
CT_NOTIFY = -1001988443538 #WI-GROUP

CT_NOTIFY_STOPRUN = 7
CT_NOTIFY_VOLUMESTOP = 6
CT_NOTIFY_HIDDENORDER = 17
CT_NOTIFY_SWEEP = 5 
CT_NOTIFY_ICEBERG = 3
CT_NOTIFY_LIQUIDITY = 16 
CT_NOTIFY_IMBALANCE = 18
CT_NOTIFY_VOLUMESPIKE = 19
CT_NOTIFY_ABSORPTION = 20
CT_NOTIFY_PRICE = 21
CT_NOTIFY_VDELTA = 22
CT_NOTIFY_MARKETVOLUME = 23
CT_NOTIFY_TREND = 3184

client = TelegramClient('influx', api_id, api_hash)
computer_name = socket.gethostname()

dataPoint = "" #data poit save to influxdb -> ex: Icebug, MV....


@client.on(events.NewMessage)
async def mess_handler(event):
    chat_id = event.chat_id
    text:str = event.raw_text
    topic = 1 #default
    # dd/mm/YY H:M:S
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    # check mes from group TTW
    if chat_id == TTW_NOTIFY or chat_id == B_TTW:
        if 'ttw top run' in text.lower() or 'top gc' in text.lower():
            topic = CT_NOTIFY_STOPRUN
            dataPoint = "Top Run"
        elif 'ttw market volume' in text.lower():
            topic = CT_NOTIFY_MARKETVOLUME
            dataPoint = "MV Stop"
        elif 'ttw volume stop' in text.lower():
            topic = CT_NOTIFY_VOLUMESTOP
            dataPoint = "Vol Stop"
        elif 'ttw hidden order' in text.lower():
            topic = CT_NOTIFY_HIDDENORDER
            dataPoint = "Hidden Order"
        elif 'ttw sweep' in text.lower():
            topic = CT_NOTIFY_SWEEP
            dataPoint = "Sweep"
        elif 'iceberg' in text.lower():
            topic = CT_NOTIFY_ICEBERG
            dataPoint = "Iceberg"
        elif 'ttw liquidity' in text.lower():
            topic = CT_NOTIFY_LIQUIDITY
            dataPoint = "Liquidity"
        elif 'ttw imbalance' in text.lower():
            topic = CT_NOTIFY_IMBALANCE
            dataPoint = "Imbalance"
        elif 'volumespike' in text.lower():
            topic = CT_NOTIFY_VOLUMESPIKE
            dataPoint = "volumespike"
        elif 'absorption' in text.lower():
            topic = CT_NOTIFY_ABSORPTION
            dataPoint = "Absorption"
        elif 'ttw price alert' in text.lower():
            topic = CT_NOTIFY_PRICE
            dataPoint = "Price"
        elif 'ttw v-delta' in text.lower():
            topic = CT_NOTIFY_VDELTA
            dataPoint = "V-delta"
        elif 'ttw trend' in text.lower():
            topic = CT_NOTIFY_TREND
            dataPoint = "Trend"

        tag = "" #BID/ASK
        vol = 0 
        price = 0
        ins=""
        lines = text.lower().split("\n")
        for line in lines:
            if 'side' in line: #BID/ASK
                tag = line.split(":")[1].strip()
            elif 'price' in line:
                price = float(line.split(":")[1].strip())
            elif 'volume' in line:
                vol = float(line.split(":")[1].strip())
            elif 'instrument' in line:
                ins = line.split(":")[1].strip()

        print(f"vol: {vol}")
        print(f"price: {price}")
        point = (
                Point(dataPoint)
                .tag("Side", tag)
                .field("Price", price).field("Vol", vol)
                .time(datetime.utcnow())
            )
        write_data(point)
def checkrunning(mess):
    client.send_message(entity=CT_NOTIFY,message= mess,reply_to=1)
def schedule_messages():
    
    #computer_name = socket.gethostname()
    schedule.every().hour.at("09:00").do(checkrunning,mess=f"Bot is running on {computer_name}")

    # Run the scheduler
    while True:
        schedule.run_pending() 
        time.sleep(1)
@client.on(events.NewMessage(pattern='/status'))
async def status(event):
    await client.send_message(entity=CT_NOTIFY,message=f"I'm running on {computer_name}",reply_to=1)

#set lich
@client.on(events.NewMessage(pattern='/schedule'))
async def schedule(event):
    await event.respond('Hello! Welcome to the bot. May i help you?')
    await client.send_message(entity=CT_NOTIFY,message=f"I'm running on {computer_name}",reply_to=1)

#Test influx 
@client.on(events.NewMessage(pattern='/savedata'))
async def savedata(event):
    await event.respond('Test save data to influxDB')
    #write_data()
    await event.respond("Saved")
#write data influxDB
def write_data(point):
    token = "acpGLbT9uxaiAFbQlFLfZA5en8Z-gldkUnYAscWk43Y7K5x8ZZb-jJdT-Zu89-aG9wllJcfsyn1msN_l-xLU2w=="
    org = "ttw"
    url = "http://10.10.13.103:8086"
    bucket="ttw"
    clientDB = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
    write_api = clientDB.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=bucket, org="ttw", record=point)
    
    #query_api = clientDB.query_api()
    #query = '''from(bucket: "ttw") |> range(start: -10m) |> filter(fn: (r) => r._measurement == "measurement1")'''
    #tables = query_api.query(query, org="ttw")
    #for table in tables:
    #    for record in table.records:    
    #        print(record)
def write_sqlite(date,Type,Ins,txt):
    conn=sqlite3.connect("ttw.db")
    cursor=conn.cursor()
    cursor.execute("INSERT INTO notify (CreatedDate,Type,Instrument,Containt) VALUES (?,?,?,?)",(date,Type,Ins,txt))
    conn.commit()
    conn.close()

client.start()
print("bot is ready.")
#schedule_messages()
client.run_until_disconnected()
