#!/usr/bin/env python3
import pika
import json
from flask import Flask, jsonify
import threading
import time
from collections import defaultdict
import os  # ADD THIS for environment variables
from datetime import datetime  # ADD THIS for timestamps

# Get port from environment variable (Azure sets this automatically)
PORT = int(os.environ.get('PORT', 8000))

# RabbitMQ connection parameters - use environment variables (SECURITY FIX)
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', '20.163.105.55')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'newuser')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD', 'newpassword')
RABBITMQ_QUEUE = os.environ.get('RABBITMQ_QUEUE', 'order_queue')

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
        order = json.loads(body)
        print(f"\n✅ [{datetime.now().isoformat()}] Received: {order['product']['name']} x{order['quantity']}")
        
        orders.append(order)
        
        name = order["product"]["name"]
        products[name]["count"] += 1
        products[name]["revenue"] += order["totalPrice"]
        products[name]["quantity"] += order["quantity"]
        
        print(f"   Total orders: {len(orders)}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def connect_to_rabbitmq():
    """Keep trying to connect and consume messages FOREVER"""
    while True:
        try:
            print("\n🔄 Connecting to RabbitMQ...")
            
            # Connect with credentials
            credentials = pika.PlainCredentials('newuser', 'newpassword')
            parameters = pika.ConnectionParameters(
                host='20.163.105.55',
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            connection = pika.BlockingConnection(parameters)
            print("✅ TCP Connection established")
            
            channel = connection.channel()
            print("✅ Channel created")
            
            # Make sure queue exists - ADD DEBUG HERE
            queue = channel.queue_declare(queue='order_queue', durable=False, passive=False)
            print(f"✅ Queue declared: {queue.method.queue}")
            print(f"   Message count: {queue.method.message_count}")
            print(f"   Consumer count: {queue.method.consumer_count}")
            
            # Check if any messages are waiting
            method_frame, header_frame, body = channel.basic_get(queue='order_queue', auto_ack=False)
            if method_frame:
                print(f"⚠️  There IS a message waiting! Attempting to process...")
                callback(channel, method_frame, header_frame, body)
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            else:
                print("ℹ️ No messages waiting in queue")
            
            print("🎯 Setting up consumer...")
            channel.basic_consume(
                queue='order_queue',
                on_message_callback=callback,
                auto_ack=True
            )
            
            print("🎯 Connected! Waiting for orders... Press Ctrl+C to stop")
            channel.start_consuming()
            
        except Exception as e:
            print(f"⚠️  Error: {type(e).__name__}: {e}")
            print("Reconnecting in 5 seconds...")
            time.sleep(5)

def start_consumer_once():
    """Ensure only one consumer thread runs with Gunicorn"""
    global _consumer_started
    with _consumer_lock:
        if not _consumer_started:
            consumer_thread = threading.Thread(target=connect_to_rabbitmq, daemon=True)
            consumer_thread.start()
            _consumer_started = True
            print(f"✅ [{datetime.now().isoformat()}] Consumer thread started")

# API Endpoints (unchanged)
@app.route('/health')
def health():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
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

if __name__ == '__main__':
    # Start RabbitMQ consumer once
    start_consumer_once()
    
    # Start Flask on the Azure-assigned port
    print(f"🚀 [{datetime.now().isoformat()}] Starting on port {PORT}")
    app.run(host='0.0.0.0', port=PORT, debug=False, threaded=True)
