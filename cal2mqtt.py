 
from http.client import REQUEST_ENTITY_TOO_LARGE
from icalendar import Calendar, Event
from datetime import *
from pytz import UTC # timezone
import numpy as np
import time
import paho.mqtt.client as mqtt
import logging
from logging.handlers  import RotatingFileHandler

TOPIC = "trash/"
BROKER_ADDRESS = "localhost"
PORT = 1883
PFAD="abfuhr.ics"

#----------------------------------------------------------------------

"""
Creates a rotating log
"""
logger = logging.getLogger("Rotating Log")
logger.setLevel(logging.DEBUG)

    # add a rotating handler
handler = RotatingFileHandler("cal2mqtt.log", maxBytes=999999,
                                backupCount=1)
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
#----------------------------------------------------------------------


logger.info("cal2mqtt Started..... ")

def on_message(client, userdata, message):
    msg = str(message.payload.decode("utf-8"))
    print("message received: ", msg)
    print("message topic: ", message.topic)
    if "set" in message.topic:
        print("set request found")
        if "path" in message.topic:
            global PFAD
            PFAD=msg.rstrip('\n')
            check_file(PFAD)
    if "get" in message.topic:
        print("get request found")
        if "gettrash" in message.topic:
            trashmsg = msg.rstrip('\n').split(",")
            for trashtype in trashmsg:
                Termin= first_event(trashtype)
               # Termin= first_event(trashtype.rstrip('\n'))
                print (msg +":" + (Termin[0]).strftime("%d.%m.%Y")+ " Tage: " + str(Termin[1]) )
        if "path" in message.topic:
            client.publish(TOPIC +"/path" , PFAD)

def on_publish(mosq, obj, mid):
    print("mid: " + str(mid))

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Verbindung akzeptiert")
        logger.info("MQTT connected: " + BROKER_ADDRESS +":"+ str(PORT) )

        client.subscribe(TOPIC+"#")
    elif rc == 1:
        print("Falsche Protokollversion")
        logger.error("MQTT not connected: Falsche Protokollversion  " + BROKER_ADDRESS +":"+ str(PORT) )
        
    elif rc == 2:
        print("Identifizierung fehlgeschlagen")
        logger.error("MQTT not connected: Auth failed " + BROKER_ADDRESS +":"+ str(PORT) )
    elif rc == 3:
        print("Server nicht erreichbar")
        logger.error("MQTT not connected: Server nicht erreichbar  " + BROKER_ADDRESS +":"+ str(PORT) )
    elif rc == 4:
        print("Falscher benutzername oder Passwort")
        logger.error("MQTT not connected: Falscher benutzername oder Passwort  " + BROKER_ADDRESS +":"+ str(PORT) )
    elif rc == 5:
        print("Nicht autorisiert")
        logger.error("MQTT not connected: " + BROKER_ADDRESS +":"+ str(PORT) )
    else:
        print("Ungültiger Returncode")
        logger.error("MQTT not connected: " + BROKER_ADDRESS +":"+ str(PORT) )
    

def check_file(datei):
    try:
        g = open(PFAD,'rb')
        gcal = Calendar.from_ical(g.read())
        client.publish(TOPIC +"/pathValid" , True)
        client.publish(TOPIC +"/path" , PFAD)
        logger.info("Path valid ICS: " + PFAD )
    except:
        print(datei + " ist nicht vorhanden oder keine gültige ICS datei")
        logger.error("Path not valid ICS: " + PFAD )
        client.publish(TOPIC +"/path" , PFAD)
        client.publish(TOPIC +"/pathValid" , False)

def first_event(name):
    utc_dt = datetime.now(timezone.utc) # UTC time
    now = utc_dt.astimezone() # local time
    termin=  now + timedelta(400)
    g = open(PFAD,'rb')
    gcal = Calendar.from_ical(g.read())
    for component in gcal.walk():
        if component.name == "VEVENT":
            if component['DTSTART'].dt > now: 
                if component.get('summary') == name:
                    if component['DTSTART'].dt < termin:            
                        termin= component['DTSTART'].dt
    tage=int( np.ceil((termin - now).days +   ((termin - now).seconds)/(60*60*24) ) )
    g.close()
    client.publish(TOPIC + name + "/date" , (termin.strftime("%d.%m.%Y")))
    client.publish(TOPIC + name + "/remDays",str(tage))
    logger.info("MQTT Published for: " + name )
    return termin,tage


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER_ADDRESS, PORT)
client.loop_start()
check_file(PFAD)

while True:
  
    time.sleep(200)





