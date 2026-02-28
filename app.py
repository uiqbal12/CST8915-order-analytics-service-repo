#!/usr/bin/env python3
import pika
import json
from flask import Flask, jsonify
import threading
import time
from collections import defaultdict
import os
from datetime import datetime
import multiprocessing

# Get port from environment variable
PORT = int(os.environ.get('PORT', 8000))

# RabbitMQ connection parameters
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', '20.163.105.55')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'newuser')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD', 'newpassword')
RABBITMQ_QUEUE = os.environ.get('RABBITMQ_QUEUE', 'order_queue')

app = Flask(__name__)

# Simple storage
orders = []
products = defaultdict(lambda: {"count": 0, "revenue": 0, "quantity": 0})

# Flag to track if consumer is running in this process
_consumer_started = False
_consumer_lock = threading.Lock()

def callback(ch, method, properties, body):
    """Process each order"""
    try:
        order = json.loads(body)
        print(f"\n✅ [{datetime.now().isoformat()}] Received: {order['product']['name']} x{order['quantity']}")
        
        orders.append(order)
        
        name = order["product"]["name"]
        products[name]["count"] += 1
        products[name]["revenue"] += order["totalPrice"]
        products[name]["quantity"] += order["quantity"]
        
        print(f"   Total orders processed: {len(orders)}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def connect_to_rabbitmq():
    """Connect and consume messages continuously"""
    while True:
        try:
            print(f"\n🔄 [{datetime.now().isoformat()}] Connecting to RabbitMQ at {RABBITMQ_HOST}...")
            print(f"   Using user: {RABBITMQ_USER}, queue: {RABBITMQ_QUEUE}")
            
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
                connection_attempts=3,
                retry_delay=2
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Declare queue and check message count
            queue = channel.queue_declare(queue=RABBITMQ_QUEUE, durable=False)
            msg_count = queue.method.message_count
            print(f"✅ [{datetime.now().isoformat()}] Connected to queue '{RABBITMQ_QUEUE}'")
            print(f"📊 Messages waiting in queue: {msg_count}")
            
            # Set up consumer
            channel.basic_consume(
                queue=RABBITMQ_QUEUE,
                on_message_callback=callback,
                auto_ack=True
            )
            
            print(f"🎯 [{datetime.now().isoformat()}] Consumer active. Waiting for orders...")
            
            # This blocks and processes messages forever
            channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError as e:
            print(f"⚠️  Connection lost: {e}")
            time.sleep(5)
        except Exception as e:
            print(f"⚠️  Error: {type(e).__name__}: {e}")
            time.sleep(5)

def start_consumer():
    """Start consumer in a background thread"""
    global _consumer_started
    with _consumer_lock:
        if not _consumer_started:
            consumer_thread = threading.Thread(target=connect_to_rabbitmq, daemon=True)
            consumer_thread.start()
            _consumer_started = True
            print(f"✅ [{datetime.now().isoformat()}] Consumer thread started in process {os.getpid()}")
        else:
            print(f"ℹ️ [{datetime.now().isoformat()}] Consumer already running in process {os.getpid()}")

# This is the key fix - run consumer when Gunicorn loads the module
print(f"🚀 [{datetime.now().isoformat()}] Loading app module in process {os.getpid()}")

# Start consumer immediately when module loads (before Gunicorn forks)
# But only in the main process, not in workers
if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
    # This runs when Gunicorn first loads the module
    print(f"📌 Starting consumer in main process {os.getpid()}")
    start_consumer()

@app.route('/health')
def health():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "process_id": os.getpid(),
        "consumer_started": _consumer_started,
        "orders_processed": len(orders),
        "products": list(products.keys()),
        "rabbitmq_config": {
            "host": RABBITMQ_HOST,
            "queue": RABBITMQ_QUEUE
        }
    })

@app.route('/orders')
def get_orders():
    return jsonify(orders)

@app.route('/analytics/summary')
def summary():
    if not orders:
        return jsonify({"totalOrders": 0, "totalRevenue": 0, "averageOrderValue": 0})
    
    total = len(orders)
    revenue = sum(o["totalPrice"] for o in orders)
    return jsonify({
        "totalOrders": total,
        "totalRevenue": round(revenue, 2),
        "averageOrderValue": round(revenue/total, 2)
    })

@app.route('/analytics/products')
def product_stats():
    result = []
    for name, stats in products.items():
        result.append({
            "productName": name,
            "orderCount": stats["count"],
            "totalRevenue": round(stats["revenue"], 2)
        })
    return jsonify(result)

@app.route('/analytics/top-products')
def top_products():
    sorted_products = sorted(
        products.items(), 
        key=lambda x: x[1]["quantity"], 
        reverse=True
    )[:3]
    
    result = []
    for name, stats in sorted_products:
        result.append({
            "productName": name,
            "totalQuantity": stats["quantity"]
        })
    return jsonify(result)

# This is for local development only
if __name__ == '__main__':
    print(f"🚀 Starting in development mode on port {PORT}")
    start_consumer()
    app.run(host='0.0.0.0', port=PORT, debug=False, threaded=True)
