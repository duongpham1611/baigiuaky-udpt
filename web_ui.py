from flask import Flask, render_template, request, jsonify, redirect, url_for
import grpc
import sys
import os
import json

# Add src to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

import service_pb2
import service_pb2_grpc

app = Flask(__name__)

# Config
NODES = [
    {"id": "node1", "address": "localhost:50051"},
    {"id": "node2", "address": "localhost:50052"},
    {"id": "node3", "address": "localhost:50053"}
]

def get_kv_stub(address):
    channel = grpc.insecure_channel(address)
    return service_pb2_grpc.KVServiceStub(channel)

def get_internal_stub(address):
    channel = grpc.insecure_channel(address)
    return service_pb2_grpc.InternalServiceStub(channel)

@app.route('/')
def index():
    return render_template('index.html', nodes=NODES)

@app.route('/api/put', methods=['POST'])
def put_key():
    data = request.json
    key = data.get('key')
    value = data.get('value')
    target_id = data.get('node_id', 'node1')
    
    target = next((n for n in NODES if n['id'] == target_id), NODES[0])
    
    try:
        stub = get_kv_stub(target['address'])
        resp = stub.Put(service_pb2.PutRequest(key=key, value=value))
        return jsonify({"success": resp.success, "message": resp.message})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route('/api/get', methods=['GET'])
def get_key():
    key = request.args.get('key')
    target_id = request.args.get('node_id', 'node1')
    target = next((n for n in NODES if n['id'] == target_id), NODES[0])

    try:
        stub = get_kv_stub(target['address'])
        resp = stub.Get(service_pb2.GetRequest(key=key))

        if resp.found:
            return jsonify({"success": True, "value": resp.value})
        else:
            return jsonify({"success": False, "message": "Key not found"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@app.route('/api/delete', methods=['POST'])
def delete_key():
    data = request.json
    key = data.get('key')
    target_id = data.get('node_id', 'node1')
    target = next((n for n in NODES if n['id'] == target_id), NODES[0])

    try:
        stub = get_kv_stub(target['address'])

        resp = stub.Delete(service_pb2.DeleteRequest(key=key))
        return jsonify({"success": resp.success, "message": resp.message})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@app.route('/api/debug/data')
def debug_data():
    # Helper to fetch all data from all nodes (using internal snapshot)
    all_data = {}
    for node in NODES:
        try:
            stub = get_internal_stub(node['address'])
            resp = stub.RequestSnapshot(service_pb2.SnapshotRequest(node_id="web_ui"), timeout=1)
            all_data[node['id']] = dict(resp.data)
        except:
             all_data[node['id']] = {"error": "Node Down"}
    return jsonify(all_data)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
