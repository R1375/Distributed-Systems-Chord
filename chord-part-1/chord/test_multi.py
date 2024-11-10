#!/usr/bin/python3

import msgpackrpc
import time
import subprocess
import signal
import sys

def new_client(ip, port):
    return msgpackrpc.Client(msgpackrpc.Address(ip, port))

def start_node(ip, port, interval=4000):
    """Start a new Chord node"""
    subprocess.Popen(["./chord", ip, str(port), str(interval)])
    time.sleep(1)  # Wait for node to start

def kill_node_by_port(port):
    """Kill a Chord node by its port"""
    subprocess.run(["pkill", "-f", f"./chord.*{port}"])
    time.sleep(1)  # Wait for the process to terminate

def kill_all_nodes():
    """Kill all running Chord nodes"""
    subprocess.run(["pkill", "-f", "chord"])
    time.sleep(1)

def show_ring_state(clients):
    """Display the current state of the Chord ring"""
    print("\nCurrent ring state:")
    for i, client in enumerate(clients):
        try:
            info = client.call("get_info")
            print(f"Node {i+1} ({info[2]}):")
            try:
                succ = client.call("get_successor")  # Directly get successor
                print(f"  Successor: {succ[2]}")
            except:
                print("  Successor: Unknown")
        except:
            print(f"Node {i+1}: Not responding")
    print()


def test_ring_formation(num_nodes=16):
    """Test creating a ring with multiple nodes"""
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


def test_lookups(clients):
    """Test lookup functionality"""
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
    """Test system behavior when nodes fail"""
    print("\n=== Testing Fault Tolerance ===")
    
    if len(clients) < 3:
        print("Need at least 3 nodes for fault tolerance test")
        return

    test_id = 123
    print(f"\nBefore failures - Looking up ID: {test_id}")
    for i, client in enumerate(clients):
        try:
            result = client.call("find_successor", test_id)
            print(f"Node {i+1} result: {result}")
        except Exception as e:
            print(f"Node {i+1} lookup failed:", e)

    # Kill middle node
    kill_index = len(clients) // 2
    node_to_kill = clients[kill_index].call("get_info")
    print(f"\nKilling Node {kill_index+1}...")
    kill_node_by_port(node_to_kill[1])
    time.sleep(40)  # Wait for stabilization

    print(f"\nAfter killing Node {kill_index+1} - Looking up ID: {test_id}")
    show_ring_state(clients)
    
    for i, client in enumerate(clients):
        if i == kill_index:
            print(f"Node {i+1}: Killed")
            continue
        try:
            result = client.call("find_successor", test_id)
            print(f"Node {i+1} result: {result}")
            print(f"✓ Node {i+1} still functioning")
        except Exception as e:
            print(f"✗ Node {i+1} failed:", e)

def main():
    try:
        kill_all_nodes()
        time.sleep(1)

        clients = test_ring_formation()
        test_lookups(clients)
        test_fault_tolerance(clients)

    except Exception as e:
        print("Test failed with error:", e)
    finally:
        print("\nCleaning up...")
        kill_all_nodes()
        print("Tests completed.")

if __name__ == "__main__":
    main()