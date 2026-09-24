from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import mysql.connector
import os
import time
import json

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "cisco.commands"
)


# ============================================================
# FastAPI
# ============================================================

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Existing MySQL model
# ============================================================

class ItemSchema(BaseModel):
    name: str


# ============================================================
# Cisco command model
# ============================================================

class CiscoCommand(BaseModel):
    command: str


# ============================================================
# MySQL
# ============================================================

def get_db_connection():

    retries = 5

    while retries > 0:

        try:

            conn = mysql.connector.connect(
                host=os.getenv("DB_HOST", "db"),
                user=os.getenv("DB_USER", "user"),
                password=os.getenv(
                    "DB_PASSWORD",
                    "userpassword"
                ),
                database=os.getenv(
                    "DB_NAME",
                    "myapp_db"
                )
            )

            return conn

        except mysql.connector.Error as e:

            print(f"MySQL connection failed: {e}")

            retries -= 1

            time.sleep(2)

    return None


# ============================================================
# Initialize database
# ============================================================

def init_db():

    conn = get_db_connection()

    if conn:

        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        conn.close()


# ============================================================
# Kafka topic
# ============================================================

def create_kafka_topic():

    print(
        f"Connecting to Kafka: "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="backend1-admin"
    )

    topic = NewTopic(
        name=KAFKA_TOPIC,
        num_partitions=1,
        replication_factor=1
    )

    try:

        admin_client.create_topics(
            new_topics=[topic]
        )

        print(
            f"Kafka topic created: {KAFKA_TOPIC}"
        )

    except TopicAlreadyExistsError:

        print(
            f"Kafka topic already exists: {KAFKA_TOPIC}"
        )

    except Exception as e:

        print(
            f"Kafka topic creation error: {e}"
        )

    finally:

        admin_client.close()


# ============================================================
# Kafka producer
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)


# ============================================================
# Startup
# ============================================================

@app.on_event("startup")
def startup():

    print("===================================")
    print("Backend 1 starting...")
    print("===================================")

    init_db()

    # Kafka may need a few seconds to start.
    # Try several times.
    for attempt in range(10):

        try:

            create_kafka_topic()

            print("Kafka is ready.")

            break

        except Exception as e:

            print(
                f"Kafka not ready "
                f"(attempt {attempt + 1}/10): {e}"
            )

            time.sleep(3)


# ============================================================
# Health check
# ============================================================

@app.get("/")
def root():

    return {
        "service": "backend1",
        "status": "running",
        "kafka_topic": KAFKA_TOPIC
    }


# ============================================================
# Existing GET items
# ============================================================

@app.get("/api/items")
def get_items():

    conn = get_db_connection()

    if not conn:

        raise HTTPException(
            status_code=500,
            detail="Database connection failed"
        )

    cursor = conn.cursor(
        dictionary=True
    )

    cursor.execute(
        "SELECT * FROM items ORDER BY id DESC"
    )

    items = cursor.fetchall()

    conn.close()

    return items


# ============================================================
# Existing POST items
# ============================================================

@app.post("/api/items")
def create_item(item: ItemSchema):

    conn = get_db_connection()

    if not conn:

        raise HTTPException(
            status_code=500,
            detail="Database connection failed"
        )

    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO items (name) VALUES (%s)",
        (item.name,)
    )

    conn.commit()

    new_id = cursor.lastrowid

    conn.close()

    return {
        "message": "Item added successfully",
        "id": new_id,
        "name": item.name
    }


# ============================================================
# Produce Cisco command
# ============================================================

@app.post("/api/cisco/command")
def send_cisco_command(command: CiscoCommand):

    if not command.command.strip():

        raise HTTPException(
            status_code=400,
            detail="Cisco command cannot be empty"
        )


    message = {
        "command": command.command.strip()
    }


    try:

        future = producer.send(
            KAFKA_TOPIC,
            value=message
        )

        metadata = future.get(
            timeout=10
        )

        producer.flush()


        print("===================================")
        print("Cisco command sent to Kafka")
        print(f"Command: {command.command}")
        print(f"Topic: {metadata.topic}")
        print(f"Partition: {metadata.partition}")
        print(f"Offset: {metadata.offset}")
        print("===================================")


        return {

            "status": "success",

            "message": "Cisco command sent to Kafka",

            "command": command.command,

            "kafka": {
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset
            }
        }


    except Exception as e:

        print(
            f"Kafka producer error: {e}"
        )

        raise HTTPException(
            status_code=500,
            detail=f"Kafka error: {str(e)}"
        )
