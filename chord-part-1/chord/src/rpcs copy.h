#ifndef RPCS_H
#define RPCS_H

#include "chord.h"
#include "rpc/client.h"
#include <array>
#include <vector>
#include <iostream>

Node self, successor, predecessor;
std::array<Node, 4> finger_table;  // m = 32 bit ids, but we use log N fingers
uint32_t next_finger = 0;
bool joined = false;
std::vector<Node> successor_list;
const int SUCCESSOR_LIST_SIZE = 5;

void debug_print(const std::string& msg) {
    std::cout << "Node " << self.id << ": " << msg << std::endl;
}

bool in_range(uint64_t id, uint64_t start, uint64_t end) {
    if (start == end) {
        return true;
    }
    if (start < end) {
        return id > start && id <= end;
    }
    return id > start || id <= end;
}

Node get_info() { return self; }

void create() {
    predecessor.ip = "";
    successor = self;
    successor_list.clear();
    successor_list.push_back(self);
    for (auto& finger : finger_table) {
        finger = self;
    }
    joined = true;
    debug_print("Created new ring");
}

Node get_predecessor() {
    return predecessor;
}

Node get_successor() {
    return successor;
}

Node closest_preceding_node(uint64_t id) {
    for (int i = finger_table.size() - 1; i >= 0; i--) {
        if (finger_table[i].ip != "" && finger_table[i].id != self.id) {
            if (self.id < id) {
                if (finger_table[i].id > self.id && finger_table[i].id < id) {
                    return finger_table[i];
                }
            } else {
                if (finger_table[i].id > self.id || finger_table[i].id < id) {
                    return finger_table[i];
                }
            }
        }
    }
    return self;
}

Node find_successor(uint64_t id) {
    if (successor.id == self.id) {
        return self;
    }

    if (predecessor.ip != "" && in_range(id, predecessor.id, self.id)) {
        return self;
    }

    if (in_range(id, self.id, successor.id)) {
        return successor;
    }

    Node n = closest_preceding_node(id);
    if (n.id == self.id) {
        return successor;
    }

    try {
        rpc::client client(n.ip, n.port);
        return client.call("find_successor", id).as<Node>();
    } catch (std::exception &e) {
        return successor;
    }
}

void notify(Node n) {
    if (predecessor.ip == "" || in_range(n.id, predecessor.id, self.id)) {
        predecessor = n;
        debug_print("Updated predecessor to " + std::to_string(n.id));
    }
}

void update_successor_list() {
    successor_list.clear();
    successor_list.push_back(successor);
    
    try {
        Node current = successor;
        for (int i = 1; i < SUCCESSOR_LIST_SIZE; i++) {
            rpc::client client(current.ip, current.port);
            current = client.call("get_successor").as<Node>();
            if (current.id == successor.id || current.id == self.id) break;
            successor_list.push_back(current);
        }
    } catch (...) {
        // Continue with partial list
    }
}

void stabilize() {
    if (!joined) return;

    try {
        // Get successor's predecessor
        Node x;
        {
            rpc::client client(successor.ip, successor.port);
            x = client.call("get_predecessor").as<Node>();
        }

        // Update successor if needed
        if (x.ip != "" && x.id != self.id) {
            bool should_update = false;
            if (successor.id == self.id) {
                should_update = true;
            } else if (in_range(x.id, self.id, successor.id)) {
                should_update = true;
            }

            if (should_update) {
                successor = x;
                finger_table[0] = x;
                debug_print("Updated successor to " + std::to_string(x.id));
                update_successor_list();  // Update list after changing successor
            }
        }

        // Always notify successor
        {
            rpc::client client(successor.ip, successor.port);
            client.call("notify", self);
        }

        // Periodically update successor list even if no change
        static int list_update_count = 0;
        if (++list_update_count >= 3) {
            list_update_count = 0;
            update_successor_list();
        }

    } catch (std::exception &e) {
        debug_print("Successor failed");
        
        // Try successor list first
        for (size_t i = 1; i < successor_list.size(); i++) {
            try {
                rpc::client client(successor_list[i].ip, successor_list[i].port);
                client.call("get_info").as<Node>();
                successor = successor_list[i];
                finger_table[0] = successor;
                debug_print("Recovered using successor list");
                update_successor_list();  // Update list after recovery
                return;
            } catch (...) {
                continue;
            }
        }

        // Then try finger table
        for (const auto& finger : finger_table) {
            if (finger.ip != "" && 
                finger.id != self.id && 
                finger.id != successor.id) {
                try {
                    rpc::client client(finger.ip, finger.port);
                    client.call("get_info").as<Node>();
                    successor = finger;
                    finger_table[0] = successor;
                    debug_print("Recovered using finger table");
                    update_successor_list();
                    return;
                } catch (...) {
                    continue;
                }
            }
        }

        // If all fails, become self-sufficient
        successor = self;
        successor_list.clear();
        successor_list.push_back(self);
        for (auto& finger : finger_table) {
            finger = self;
        }
        debug_print("No viable successor found");
    }
}

void join(Node n) {
    predecessor.ip = "";
    joined = false;
    debug_print("Joining through node " + std::to_string(n.id));
    
    try {
        // Find initial successor
        {
            rpc::client client(n.ip, n.port);
            successor = client.call("find_successor", self.id).as<Node>();
            debug_print("Found successor " + std::to_string(successor.id));
        }

        // Initialize finger table
        finger_table[0] = successor;
        for (size_t i = 1; i < finger_table.size(); i++) {
            uint64_t start = (self.id + (1ULL << i)) % ((1ULL << 32) - 1);
            try {
                rpc::client client(n.ip, n.port);
                finger_table[i] = client.call("find_successor", start).as<Node>();
            } catch (...) {
                finger_table[i] = successor;
            }
        }

        // Initialize successor list
        successor_list.clear();
        successor_list.push_back(successor);
        try {
            Node current = successor;
            for (int i = 1; i < SUCCESSOR_LIST_SIZE; i++) {
                rpc::client client(current.ip, current.port);
                current = client.call("get_successor").as<Node>();
                if (current.id == successor.id || current.id == self.id) break;
                successor_list.push_back(current);
            }
        } catch (...) {}

        joined = true;
        debug_print("Successfully joined ring");

        // Notify successor after fully joined
        try {
            rpc::client client(successor.ip, successor.port);
            client.call("notify", self);
        } catch (...) {}

    } catch (std::exception &e) {
        debug_print("Join failed, becoming self-sufficient");
        successor = self;
        successor_list.clear();
        successor_list.push_back(self);
        for (auto& finger : finger_table) {
            finger = self;
        }
        joined = true;
    }
}

void fix_fingers() {
    if (!joined || successor.id == self.id) return;
    
    static int skip_count = 0;
    if (++skip_count < 3) return;
    skip_count = 0;
    
    try {
        uint64_t start = (self.id + (1ULL << next_finger)) % ((1ULL << 32) - 1);
        Node current = successor;
        
        // Try to use existing fingers to get closer
        for (int i = finger_table.size() - 1; i >= 0; i--) {
            if (finger_table[i].ip != "" && 
                in_range(finger_table[i].id, self.id, start)) {
                current = finger_table[i];
                break;
            }
        }
        
        rpc::client client(current.ip, current.port);
        Node new_finger = client.call("find_successor", start).as<Node>();
        
        if (finger_table[next_finger].id != new_finger.id) {
            finger_table[next_finger] = new_finger;
            debug_print("Updated finger " + std::to_string(next_finger));
        }
    } catch (...) {
        finger_table[next_finger] = successor;
    }
    
    next_finger = (next_finger + 1) % finger_table.size();
}

void check_predecessor() {
    if (!joined || predecessor.ip == "") return;
    
    try {
        rpc::client client(predecessor.ip, predecessor.port);
        client.call("get_info").as<Node>();
    } catch (std::exception &e) {
        predecessor.ip = "";
        debug_print("Predecessor failed, cleared");
    }
}

void register_rpcs() {
    add_rpc("get_info", &get_info);
    add_rpc("create", &create);
    add_rpc("join", &join);
    add_rpc("find_successor", &find_successor);
    add_rpc("notify", &notify);
    add_rpc("get_predecessor", &get_predecessor);
    add_rpc("get_successor", &get_successor);
}

void register_periodics() {
    add_periodic(check_predecessor);
    add_periodic(stabilize);
    add_periodic(fix_fingers);
}

#endif /* RPCS_H */