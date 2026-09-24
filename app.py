from flask import Flask, render_template, request, jsonify
import paramiko
import os
import json
import threading
import logging
from kafka import KafkaConsumer

app = Flask(__name__)

# --------------------------------------------------
# Configuration
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "cisco.commands"
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "cisco-router-worker"
)

ROUTER_IP = os.getenv(
    "ROUTER_IP",
    "192.168.235.133"
)

ROUTER_USERNAME = os.getenv(
    "ROUTER_USERNAME",
    "admin"
)

ROUTER_PASSWORD = os.getenv(
    "ROUTER_PASSWORD",
    "cisco"
)

# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# Cisco SSH command execution
# --------------------------------------------------

def execute_cisco_command(command):
    """
    Connect to the Cisco router using SSH,
    execute the command, and return the output.
    """

    logger.info(
        "Connecting to Cisco router %s",
        ROUTER_IP
    )

    ssh = None

    try:
        ssh = paramiko.SSHClient()

        # Accept the router host key automatically.
        # Fine for this lab environment.
        ssh.set_missing_host_key_policy(
            paramiko.AutoAddPolicy()
        )

        ssh.connect(
            hostname=ROUTER_IP,
            username=ROUTER_USERNAME,
            password=ROUTER_PASSWORD,
            timeout=10,
            look_for_keys=False,
            allow_agent=False
        )

        logger.info(
            "Connected to Cisco router %s",
            ROUTER_IP
        )

        logger.info(
            "Executing Cisco command: %s",
            command
        )

        stdin, stdout, stderr = ssh.exec_command(command)

        output = stdout.read().decode(
            "utf-8",
            errors="ignore"
        )

        error = stderr.read().decode(
            "utf-8",
            errors="ignore"
        )

        if error:
            logger.warning(
                "Cisco command stderr: %s",
                error
            )

        result = output if output else error

        logger.info(
            "Cisco command completed"
        )

        if result:
            logger.info(
                "Cisco output:\n%s",
                result
            )

        return result

    except Exception as e:
        logger.error(
            "Cisco command failed: %s",
            str(e)
        )
        raise

    finally:
        if ssh:
            ssh.close()
            logger.info(
                "Cisco SSH connection closed"
            )


# --------------------------------------------------
# Kafka Worker
# --------------------------------------------------

def kafka_worker():

    logger.info(
        "Starting Kafka worker..."
    )

    logger.info(
        "Kafka server: %s",
        KAFKA_BOOTSTRAP_SERVERS
    )

    logger.info(
        "Kafka topic: %s",
        KAFKA_TOPIC
    )

    logger.info(
        "Kafka group: %s",
        KAFKA_GROUP_ID
    )

    while True:

        consumer = None

        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,

                bootstrap_servers=[
                    KAFKA_BOOTSTRAP_SERVERS
                ],

                group_id=KAFKA_GROUP_ID,

                auto_offset_reset="earliest",

                enable_auto_commit=False,

                value_deserializer=lambda value:
                    json.loads(
                        value.decode("utf-8")
                    ),

                consumer_timeout_ms=1000
            )

            logger.info(
                "Connected to Kafka successfully"
            )

            # Keep consuming messages
            while True:

                records = consumer.poll(
                    timeout_ms=1000
                )

                if not records:
                    continue

                for topic_partition, messages in records.items():

                    for message in messages:

                        logger.info(
                            "Received Kafka message: "
                            "topic=%s partition=%s offset=%s",
                            message.topic,
                            message.partition,
                            message.offset
                        )

                        try:
                            data = message.value

                            logger.info(
                                "Message data: %s",
                                data
                            )

                            command = data.get(
                                "command"
                            )

                            if not command:
                                logger.error(
                                    "Kafka message does not contain "
                                    "'command'"
                                )

                                # Skip malformed message.
                                consumer.commit()

                                continue

                            logger.info(
                                "Executing command from Kafka: %s",
                                command
                            )

                            # Execute command on Cisco router
                            output = execute_cisco_command(
                                command
                            )

                            logger.info(
                                "Cisco command output:\n%s",
                                output
                            )

                            # Commit only after successful execution
                            consumer.commit()

                            logger.info(
                                "Kafka message committed successfully"
                            )

                        except Exception as e:

                            logger.error(
                                "Failed to process Kafka message: %s",
                                str(e)
                            )

                            logger.error(
                                "Message will not be committed."
                            )

        except Exception as e:

            logger.error(
                "Kafka worker connection error: %s",
                str(e)
            )

            logger.info(
                "Retrying Kafka connection in 5 seconds..."
            )

        finally:

            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass

        import time
        time.sleep(5)


# --------------------------------------------------
# Existing Web UI
# --------------------------------------------------

@app.route('/')
def index():
    return render_template(
        'index.html'
    )


# --------------------------------------------------
# Existing direct HTTP command endpoint
# --------------------------------------------------

@app.route('/execute', methods=['POST'])
def execute():

    data = request.get_json()

    if not data:
        return jsonify({
            'error': 'JSON body is required.'
        }), 400

    ip = data.get(
        'ip',
        ROUTER_IP
    )

    command = data.get(
        'command'
    )

    username = data.get(
        'username',
        ROUTER_USERNAME
    )

    password = data.get(
        'password',
        ROUTER_PASSWORD
    )

    if not ip or not command:
        return jsonify({
            'error': 'Router IP and command are required.'
        }), 400

    ssh = None

    try:

        ssh = paramiko.SSHClient()

        ssh.set_missing_host_key_policy(
            paramiko.AutoAddPolicy()
        )

        ssh.connect(
            hostname=ip,
            username=username,
            password=password,
            timeout=10,
            look_for_keys=False,
            allow_agent=False
        )

        stdin, stdout, stderr = ssh.exec_command(
            command
        )

        output = stdout.read().decode(
            'utf-8',
            errors='ignore'
        )

        error = stderr.read().decode(
            'utf-8',
            errors='ignore'
        )

        result = output if output else error

        return jsonify({
            'output': (
                result
                if result
                else 'Command executed with no output.'
            )
        })

    except Exception as e:

        logger.error(
            "Direct HTTP command failed: %s",
            str(e)
        )

        return jsonify({
            'error': str(e)
        }), 500

    finally:

        if ssh:
            ssh.close()


# --------------------------------------------------
# Start Kafka worker
# --------------------------------------------------

if __name__ == '__main__':

    worker_thread = threading.Thread(
        target=kafka_worker,
        daemon=True
    )

    worker_thread.start()

    logger.info(
        "Kafka worker thread started"
    )

    app.run(
        host='0.0.0.0',
        port=5000
    )
