import json
import time
from pathlib import Path
import sys

import pika  # type: ignore
import random

# callback (process control) - {process-deal / publish(Fail)-pushback / dead} - callback (ack)

# 让 worker 可以导入 code/shared 里的文件
# current path -→ absolute path -→ back 2 layer: to /code
BASE_DIR = Path(__file__).resolve().parents[1]
# put in Python search module
sys.path.append(str(BASE_DIR))


def process_message(body: bytes, service_name: str):

    # RabbitMQ message (bytes type)
    # bytes -→ JSON -→ dict (producer: body=json.dumps(message).encode("utf-8"))
    message = json.loads(body.decode("utf-8"))

    # read or set null (message["xx"] will error when null)
    request_id = message.get("request_id", "")
    notification_id = message.get("notification_id", "")
    trace_id = message.get("trace_id", "")
    channel = message.get("channel", "")
    priority = message.get("priority", "")

    # read fail properties
    force_fail = message.get("force_fail", False)
    force_timeout = message.get("force_timeout", False)
    enable_random_fail = message.get("enable_random_fail", False)
    enable_random_fail_rate = message.get("enable_random_fail_rate", 0.3)

    # Counting for terminal
    retry_count = message.get("retry_count", 0)

    # record current time (secend) -→ for calculate total time cost
    start_time = time.time()

    # log processing, print
    if retry_count == 0:
        attempt_label = "FIRST TRY"
    else:
        attempt_label = f"RETRY {retry_count}"

    print(
        f"\n[{attempt_label}] [{service_name}] START | "
        f"request={request_id} | notif={notification_id} | "
        f"channel={channel} | priority={priority}"
    )

    log_event(
        service_name=service_name,
        level="INFO",
        event="notification_processing",
        status="processing",
        request_id=request_id,
        notification_id=notification_id,
        trace_id=trace_id,
        channel=channel,
        priority=priority,
        latency_ms=0,
    )

    # filelock: metrics_processed
    """ [lambda metrics:]-→ temporary fn named metrics
        [metrics.__setitem__("A",B)] -→ metrics["A"]=B"""
    update_metrics(
        lambda metrics: metrics.__setitem__(
            "notifications_processing_attempts_total",
            metrics["notifications_processing_attempts_total"] + 1,
        )
    )

    # Fail scenarios simulation
    if force_fail:
        raise RuntimeError("Simulated delivery failure")
    if force_timeout:
        time.sleep(TIMEOUT_SECONDS + 3)
        raise TimeoutError(
            f"Simulated timeout: processing exceeded {TIMEOUT_SECONDS} seconds"
        )
    if enable_random_fail and random.random() < enable_random_fail_rate:
        raise RuntimeError("Random Simulated delivery failure")

    # Normal scenario
    # 模拟发送耗时
    time.sleep(PROCESSING_SECONDS)

    # calculate cost time
    latency_ms = int((time.time() - start_time) * 1000)  # ms

    # log delivered, print
    log_event(
        service_name=service_name,
        level="INFO",
        event="notification_delivered",
        status="delivered",
        request_id=request_id,
        notification_id=notification_id,
        trace_id=trace_id,
        channel=channel,
        priority=priority,
        latency_ms=latency_ms,  # 1000-
    )

    # filelock: metrics_delivered
    def delivered_update(metrics):
        metrics["notifications_delivered_total"] += 1
        metrics["delivery_latency_ms_total"] += latency_ms

    update_metrics(delivered_update)

    print(f"[{attempt_label}] [{service_name}] END -> DELIVERED")
    print("=" * 60)


# Fail: retry_publish
def publish_message(channel, queue_name, message: dict):
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=json.dumps(message).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2),
    )


# Modification: main(){xxx} -→ run_worker {mainHearder + callback + mainTail},
# ...and put out publish_message(retry)
def run_worker(service_name: str, queue_name: str):

    # Step: 1.Connect RabbitMQ 2.Declare queue 3.define callback 4.start_consuming
    # Set parameter
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
    )

    # Connection
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # create if not existing: durable
    channel.queue_declare(queue=queue_name, durable=True)

    print(f"[{service_name}] waiting for messages from {queue_name}...")

    # AutoCall (such as process_message) once RabbitMQ send message
    # channel(ack,nack);method(delivery_tag);properties(headers, durable,xx);body
    def callback(ch, method, properties, body):

        # RabbitMQ message (bytes type)
        # bytes -→ JSON -→ dict
        message = json.loads(body.decode("utf-8"))

        request_id = message.get("request_id", "")
        notification_id = message.get("notification_id", "")
        trace_id = message.get("trace_id", "")
        channel_name = message.get("channel", "")
        priority = message.get("priority", "")

        retry_count = message.get("retry_count", 0)
        max_retries = message.get("max_retries", MAX_RETRIES)

        try:
            process_message(body, service_name)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            log_event(
                service_name=service_name,
                level="ERROR",
                event="notification_failed",
                status="failed",
                request_id=request_id,
                notification_id=notification_id,
                trace_id=trace_id,
                channel=channel_name,
                priority=priority,
                latency_ms=0,
            )

            update_metrics(
                lambda metrics: metrics.__setitem__(
                    "notifications_failed_total",
                    metrics["notifications_failed_total"] + 1,
                )
            )

            print(f"[{service_name}] error: {e}")
