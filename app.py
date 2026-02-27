#!/usr/bin/env python3
import pika
import json
from flask import Flask, jsonify
import threading
import time
from collections import defaultdict

app = Flask(__name__)

# Simple storage
orders = []
products = defaultdict(lambda: {"count": 0, "revenue": 0, "quantity": 0})

def callback(ch, method, properties, body):
    """Process each order - this runs for EVERY message"""
    try:
        # Parse the message
        order = json.loads(body)
        print(f"\n✅ Received: {order['product']['name']} x{order['quantity']}")
        
        # Store the full order
        orders.append(order)
        
        # Update product stats
        name = order["product"]["name"]
        products[name]["count"] += 1
        products[name]["revenue"] += order["totalPrice"]
        products[name]["quantity"] += order["quantity"]
        
        print(f"   Stats so far:")
        for product_name, stats in products.items():
            print(f"   - {product_name}: {stats['quantity']} units sold")
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
                host='20.38.3.244',
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Make sure queue exists
            channel.queue_declare(queue='order_queue', durable=False)
            
            # Clear any existing messages (optional - remove if you want to keep old messages)
            # channel.queue_purge(queue='order_queue')
            
            print("🎯 Connected! Waiting for orders... Press Ctrl+C to stop")
            
            # Start consuming - this blocks and processes messages one by one
            # The callback runs for EACH message
            channel.basic_consume(
                queue='order_queue',
                on_message_callback=callback,
                auto_ack=True  # Automatically confirm message processing
            )
            
            # This will keep running forever, processing messages as they arrive
            channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError as e:
            print(f"⚠️  Connection lost: {e}")
            print("Reconnecting in 5 seconds...")
            time.sleep(5)
        except pika.exceptions.ChannelClosedByBroker as e:
            print(f"⚠️  Channel error: {e}")
            print("Reconnecting in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"⚠️  Unexpected error: {e}")
            print("Reconnecting in 5 seconds...")
            time.sleep(5)

# API Endpoints
@app.route('/health')
def health():
    return jsonify({
        "status": "ok", 
        "orders_processed": len(orders),
        "products": list(products.keys())
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
    # Start RabbitMQ consumer in background thread
    consumer_thread = threading.Thread(target=connect_to_rabbitmq, daemon=True)
    consumer_thread.start()
    
    # Start Flask
    print("🚀 Starting Order Analytics Service on port 4000")
    app.run(host='0.0.0.0', port=4000, debug=False, threaded=True)
