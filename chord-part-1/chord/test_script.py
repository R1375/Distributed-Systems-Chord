#!/usr/bin/python3

import msgpackrpc
import time
import subprocess
import sys
import random
from collections import defaultdict

# 消息計數器
class MessageCounter:
    def __init__(self):
        self.reset()
    
    def record_message(self, msg_type):
        self.counts[msg_type] += 1
        self.total_count += 1
        self.record_timestamp()
        
    def record_timestamp(self):
        current_time = time.time()
        self.timestamps.append(current_time)
        while self.timestamps and self.timestamps[0] < current_time - 1:
            self.timestamps.pop(0)
    
    def get_rate(self):
        return len(self.timestamps)
    
    def get_count(self):
        return self.total_count
    
    def reset(self):
        self.counts = defaultdict(int)
        self.total_count = 0
        self.timestamps = []

# RPC包裝器
class RPCWrapper:
    def __init__(self, ip, port, msg_counter):
        self.client = msgpackrpc.Client(msgpackrpc.Address(ip, port))
        self.msg_counter = msg_counter
        
    def call(self, method, *args):
        self.msg_counter.record_message(method)
        return self.client.call(method, *args)

msg_counter = MessageCounter()

def new_client(ip, port):
    return RPCWrapper(ip, port, msg_counter)

def start_node(ip, port, interval=2000):
    subprocess.Popen(["./chord", ip, str(port), str(interval)])
    time.sleep(1)

def kill_all_nodes():
    subprocess.run(["pkill", "-f", "chord"])
    time.sleep(1)

def kill_node_by_port(port):
    subprocess.run(["pkill", "-f", f"./chord.*{port}"])
    time.sleep(1)

def show_ring_state(clients):
    print("\nCurrent ring state:")
    for i, client in enumerate(clients):
        try:
            info = client.call("get_info")
            print(f"Node {i+1} ({info[2]}):")
            try:
                succ = client.call("find_successor", info[2])
                print(f"  Successor: {succ[2]}")
            except:
                print("  Successor: Unknown")
        except:
            print(f"Node {i+1}: Not responding")
    print()

def test_ring_formation(num_nodes=16):  # 改為16個節點用於測試Message Complexity
    print(f"\n=== Testing Ring Formation with {num_nodes} nodes ===")
    
    base_port = 5056
    clients = []
    
    print("\nStarting nodes...")
    for i in range(num_nodes):
        port = base_port + i
        start_node("127.0.0.1", port)
        client = new_client("127.0.0.1", port)
        clients.append(client)
        print(f"Node {i+1} info:", client.call("get_info"))

    print("\nCreating ring with Node 1...")
    clients[0].call("create")
    time.sleep(2)

    print("\nJoining other nodes to the ring...")
    for i in range(1, num_nodes):
        print(f"Node {i+1} joining...")
        clients[i].call("join", clients[0].call("get_info"))
        time.sleep(2)
        show_ring_state(clients[:i+1])

    print("\nWaiting for ring to stabilize...")
    time.sleep(20)
    show_ring_state(clients)
    
    return clients

def test_message_complexity(clients):
    print("\n=== Testing Message Complexity ===")
    
    # 1. 測試 find_successor 消息複雜度
    print("\nTesting find_successor message complexity...")
    num_requests = 50  # 執行50次取平均
    successful_lookups = 0
    total_messages = 0
    
    test_ids = [random.randint(0, 2**32) for _ in range(num_requests)]
    
    for i, test_id in enumerate(test_ids):
        msg_counter.reset()
        client = random.choice(clients)  # 隨機選擇節點
        
        try:
            client.call("find_successor", test_id)
            messages = msg_counter.get_count()
            total_messages += messages
            successful_lookups += 1
            print(f"Lookup {i+1} used {messages} messages")
        except Exception as e:
            print(f"Lookup {i+1} failed: {e}")
    
    if successful_lookups > 0:
        avg_messages = total_messages / successful_lookups
        print(f"\nFind_successor statistics:")
        print(f"Average messages per lookup: {avg_messages:.2f}")
        if avg_messages < 7:
            print("✓ Message complexity OK (< 7 messages per lookup)")
        else:
            print("✗ Message complexity too high (>= 7 messages per lookup)")
    
    # 2. 測試週期性函數消息複雜度
    print("\nTesting periodic function message complexity...")
    msg_counter.reset()
    time.sleep(5)  # 觀察5秒
    
    rate = msg_counter.get_rate()
    print(f"RPC calls per second: {rate:.2f}")
    if rate < 64:
        print("✓ Periodic message rate OK (< 64 messages/second)")
    else:
        print("✗ Periodic message rate too high (>= 64 messages/second)")
    
    print("\nMessage type distribution:")
    for msg_type, count in msg_counter.counts.items():
        print(f"{msg_type}: {count}")

def test_lookups(clients):
    print("\n=== Testing Lookups ===")
    
    node_ids = []
    for client in clients:
        try:
            info = client.call("get_info")
            node_ids.append(info[2])
        except:
            continue
            
    print("Node IDs in ring:", node_ids)
    
    test_ids = [123, 2**31, 2**32 - 1] + node_ids
    
    for test_id in test_ids:
        print(f"\nLooking up successor for ID: {test_id}")
        results = {}
        
        for i, client in enumerate(clients):
            try:
                result = client.call("find_successor", test_id)
                results[i] = result
                print(f"Node {i+1} ({client.call('get_info')[2]}) returned: {result[2]}")
            except Exception as e:
                print(f"Node {i+1} lookup failed:", e)
        
        if len(set(str(r) for r in results.values())) == 1:
            print("✓ All nodes returned same successor")
        else:
            print("✗ Nodes returned different successors!")
            show_ring_state(clients)

def test_fault_tolerance(clients):
    print("\n=== Testing Fault Tolerance ===")

    test_id = 123
    print("\nBefore failures - Looking up ID:", test_id)
    for i, client in enumerate(clients):
        try:
            result = client.call("find_successor", test_id)
            print(f"Node {i+1} result:", result)
        except Exception as e:
            print(f"Node {i+1} lookup failed:", e)

    kill_index = len(clients) // 2
    node_to_kill = clients[kill_index].call("get_info")
    print(f"\nKilling Node {kill_index+1}...")
    kill_node_by_port(node_to_kill[1])
    time.sleep(40)

    print(f"\nAfter killing Node {kill_index+1} - Looking up ID: {test_id}")
    show_ring_state(clients)
    
    for i, client in enumerate(clients):
        if i == kill_index:
            continue
        try:
            result = client.call("find_successor", test_id)
            print(f"Node {i+1} result:", result)
            print(f"✓ Node {i+1} still functioning")
        except Exception as e:
            print(f"✗ Node {i+1} failed:", e)

def main():
    try:
        kill_all_nodes()
        time.sleep(1)

        # Part 1: Basic correctness with message complexity testing
        clients = test_ring_formation(16)  # 使用16個節點
        test_lookups(clients)
        test_message_complexity(clients)  # 添加消息複雜度測試

        # Part 2: Fault tolerance testing
        test_fault_tolerance(clients)

    except Exception as e:
        print("Test failed with error:", e)
    finally:
        print("\nCleaning up...")
        kill_all_nodes()
        print("Tests completed.")

if __name__ == "__main__":
    main()