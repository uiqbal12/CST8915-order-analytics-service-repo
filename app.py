#!/usr/bin/env python3
import pika
import json
from flask import Flask, jsonify
import threading
import time
from collections import defaultdict
import os
from datetime import datetime  # ADD THIS

# RabbitMQ connection parameters - read from environment variables
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD', 'guest')
RABBITMQ_QUEUE = os.environ.get('RABBITMQ_QUEUE', 'order_queue')

# Get port from environment (Azure sets this automatically)
PORT = int(os.environ.get('PORT', 8000))  # Azure default is 8000

app = Flask(__name__)

# Simple storage
orders = []
products = defaultdict(lambda: {"count": 0, "revenue": 0, "quantity": 0})

# Thread safety for Gunicorn multiple workers
_consumer_started = False
_consumer_lock = threading.Lock()

def callback(ch, method, properties, body):
    """Process each order - this runs for EVERY message"""
    try:
        # Parse the message
        order = json.loads(body)
        print(f"\n✅ [{datetime.now().isoformat()}] Received: {order['product']['name']} x{order['quantity']}")
        
        # Store the full order
        orders.append(order)
        
        # Update product stats
        name = order["product"]["name"]
        products[name]["count"] += 1
        products[name]["revenue"] += order["totalPrice"]
        products[name]["quantity"] += order["quantity"]
        
        print(f"   Total orders: {len(orders)}")
        
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
    except KeyError as e:
        print(f"❌ Missing key in order: {e}")
    except Exception as e:
        print(f"❌ Error processing order: {e}")

def connect_to_rabbitmq():
    """Keep trying to connect and consume messages FOREVER"""
    while True:
        try:
            print(f"\n🔄 [{datetime.now().isoformat()}] Connecting to RabbitMQ at {RABBITMQ_HOST}:5672...")
            print(f"   Using user: {RABBITMQ_USER}, queue: {RABBITMQ_QUEUE}")
            
            # Connect with credentials
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
            
            # Check if queue exists (passive=True doesn't create it)
            try:
                channel.queue_declare(queue=RABBITMQ_QUEUE, durable=False, passive=True)
                print(f"   ✅ Queue '{RABBITMQ_QUEUE}' exists")
            except pika.exceptions.ChannelClosedByBroker:
                # Queue doesn't exist, create it
                print(f"   Queue '{RABBITMQ_QUEUE}' doesn't exist, creating...")
                channel = connection.channel()  # Need new channel after error
                channel.queue_declare(queue=RABBITMQ_QUEUE, durable=False)
                print(f"   ✅ Queue '{RABBITMQ_QUEUE}' created")
            
            print(f"🎯 [{datetime.now().isoformat()}] SUCCESS: Connected to RabbitMQ!")
            print(f"   Listening on queue: {RABBITMQ_QUEUE}")
            
            channel.basic_consume(
                queue=RABBITMQ_QUEUE,
                on_message_callback=callback,
                auto_ack=True
            )
            
            # This blocks until connection is closed
            channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError as e:
            print(f"❌ [{datetime.now().isoformat()}] Connection failed: {e}")
            print(f"   Reconnecting in 5 seconds...")
            time.sleep(5)
        except pika.exceptions.ProbableAccessDeniedError as e:
            print(f"❌ [{datetime.now().isoformat()}] Authentication failed: {e}")
            print(f"   Check username/password for user '{RABBITMQ_USER}'")
            time.sleep(10)
        except Exception as e:
            print(f"❌ [{datetime.now().isoformat()}] Unexpected error: {type(e).__name__}: {e}")
            print(f"   Reconnecting in 5 seconds...")
            time.sleep(5)

def start_consumer_once():
    """Ensure only one consumer thread is started across all workers"""
    global _consumer_started
    with _consumer_lock:
        if not _consumer_started:
            consumer_thread = threading.Thread(target=connect_to_rabbitmq, daemon=True)
            consumer_thread.start()
            _consumer_started = True
            print(f"✅ [{datetime.now().isoformat()}] Consumer thread started")
        else:
            print(f"ℹ️ [{datetime.now().isoformat()}] Consumer already running, skipping...")

# API Endpoints
@app.route('/health')
def health():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "orders_processed": len(orders),
        "products": list(products.keys()),
        "rabbitmq_config": {
            "host": RABBITMQ_HOST,
            "queue": RABBITMQ_QUEUE,
            "user": RABBITMQ_USER
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

# Diagnostic endpoint to test RabbitMQ
@app.route('/diagnostic/rabbitmq')
def diagnostic_rabbitmq():
    """Test RabbitMQ connection without consuming"""
    import socket
    
    results = {
        "timestamp": datetime.now().isoformat(),
        "config": {
            "host": RABBITMQ_HOST,
            "port": 5672,
            "user": RABBITMQ_USER,
            "queue": RABBITMQ_QUEUE
        },
        "tests": {}
    }
    
    # Test socket connection
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((RABBITMQ_HOST, 5672))
        results["tests"]["socket"] = {
            "success": result == 0,
            "message": "Port reachable" if result == 0 else f"Failed with code {result}"
        }
        sock.close()
    except Exception as e:
        results["tests"]["socket"] = {"success": False, "message": str(e)}
    
    # Test RabbitMQ auth if socket works
    if results["tests"]["socket"]["success"]:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                credentials=credentials,
                connection_attempts=1,
                retry_delay=1
            )
            connection = pika.BlockingConnection(parameters)
            connection.close()
            results["tests"]["auth"] = {"success": True, "message": "Authentication successful"}
        except Exception as e:
            results["tests"]["auth"] = {"success": False, "message": str(e)}
    
    return jsonify(results)

if __name__ == '__main__':
    # Start RabbitMQ consumer in background thread (only once)
    start_consumer_once()
    
    # Start Flask - use PORT from environment
    print(f"🚀 [{datetime.now().isoformat()}] Starting Order Analytics Service on port {PORT}")
    app.run(host='0.0.0.0', port=PORT, debug=False, threaded=True)
