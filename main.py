import socket
import random
import json
import time
import threading
import multiprocessing
import pickle
import sys
import copy

class node():
    def __init__(self, num_conn, end_sim):
        self.num_conn = num_conn
        self.end_sim = end_sim
        self.node_num = -1 ##It will be set in Run Method
        self.sleep_notif = None ##It will be set in Run Method
        self.hello_sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.hello_sock.bind(('0.0.0.0', 0))
        self.hello_sock.setblocking(0)
        self.conn_sockets = [socket.socket() for _ in range(num_conn)] ##for connection between adjacent host
        for i in self.conn_sockets:
            i.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.total_time = 0
        self.host_addr_list = []
        self.access_count = []
        self.adj_addr_list = []
        self.adj_history_set = set()
        self.recv_data = [None for _ in range(num_conn)]
        self.last_recv_time = {}
        self.last_send_time = {}
        self.num_data_send = {}



    def extract_connections(self, adj_idx):
        data = self.recv_data[adj_idx]
        try:
            return data["Adjacents"]
        except:
            print(data, "****", self.adj_addr_list, "***", "***", self.recv_data, adj_idx)
            return None

    def random_adj_addr(self, exclude_addr=[]):
        addr_list = copy.deepcopy(self.host_addr_list)
        addr_list.remove(self.hello_sock.getsockname())
        # print(addr_list)
        while(1):
            try:
                addr = random.choice(addr_list)
                if addr[1] in [i[1] for i in exclude_addr]:
                    addr_list.remove(addr)
                    continue
                return addr
            except:
                print(f'Something is wrong with random_adj_addr function, host_list = {self.host_addr_list}, \
                    addr = {addr}, addr_list = {addr_list}')
                print(sys.exc_info()[0])
                sys.exit(1)


    def set_host_list(self, host_addr):
        self.host_addr_list = host_addr
        for addr in host_addr:
            self.num_data_send[self.correspond_idx(addr, self.host_addr_list)] = [0, 0]
            self.last_recv_time[addr[1]] = 0
            self.last_send_time[addr[1]] = 0
        self.access_count = [0 for _ in range(len(self.host_addr_list))]
        while len(self.adj_addr_list) != self.num_conn:
            addr = self.random_adj_addr(exclude_addr=self.adj_addr_list)
            self.adj_addr_list.append(addr)
        # print(self.adj_addr_list)
    def do_stuff(self):
        self.end_sim.wait()

    def make_hello_msg(self):
        adj_nodes = []
        for i in self.adj_addr_list:
            idx = self.correspond_idx(i, self.host_addr_list)
            if idx < 0:
                print("Error in make_hello_msg function")
            adj_nodes.append(idx)
        msg = {"Message Type" : "HELLO"}
        msg["Adjacents"] = adj_nodes
        msg["Node ID"] = self.node_num
        # msg = json.dumps(msg)
        return msg

    def correspond_idx(self, addr, search_list = None):
        if not search_list:
            search_list = self.adj_addr_list

        for idx, addr_ in enumerate(search_list):
            # print(addr_, addr)
            if addr_[1] == addr[1]:
                return idx
        return -1

    def process_recv_data(self):
        hello_recv = [False for _ in range(self.num_conn)]
        while(1):
            try:
                data, addr = self.hello_sock.recvfrom(2048)## Raise error if Buffer is Empty
                if not data:
                    continue
                data = pickle.loads(data)
                data = json.loads(data)
                idx = self.correspond_idx(addr, self.adj_addr_list)
                if idx >= 0: 
                    hello_recv[idx] = True
                    # print(data)
                    self.recv_data[idx] = data
            except:
                break

        adj_addr = [self.adj_addr_list[idx] for idx, i in enumerate(hello_recv) if i==True]
        for idx, val in enumerate(hello_recv):
            addr = self.adj_addr_list[idx]
            if val == False:
                addr = self.random_adj_addr(exclude_addr=adj_addr)
                adj_addr.append(addr)
            else:
                host_idx = self.correspond_idx(addr, self.host_addr_list)
                self.last_recv_time[addr[1]] = self.total_time
                self.num_data_send[host_idx][1] += 1
                self.adj_history_set.add(host_idx)
                self.access_count[host_idx] += 1

        self.adj_addr_list = adj_addr

    def send_hello(self):
        for idx, addr in enumerate(self.adj_addr_list):
            host_idx = self.correspond_idx(addr, self.host_addr_list)
            data = self.make_hello_msg() #return dict
            data["Sender Address"] = self.hello_sock.getsockname()
            data["Receiver Address"] = addr
            data["Last Receive Time"] = self.last_recv_time[addr[1]]
            data["Last Send Time"] = self.last_send_time[addr[1]]
            # data["Number of Data "]
            # print(data)
            data = json.dumps(data)
            data = pickle.dumps(data)
            self.num_data_send[host_idx][0] += 1
            self.last_send_time[addr[1]] = self.total_time
            self.hello_sock.sendto(data, addr)


    def hello_process(self):
        counter = 0
        while(1):
            if self.sleep_notif.is_set():
                self.sleep_notif.clear()
                time.sleep(18)
                self.total_time += 2.5
                counter = -1
            time.sleep(2)
            counter += 1
            if counter == 4:
                self.process_recv_data()
                counter = 0
                self.total_time += 1
                if self.end_sim.is_set():
                    break
            self.send_hello()


    def run(self, node_num, sleep_notif):
        self.node_num = node_num
        self.sleep_notif = sleep_notif ## Make Node sleep for 20s
        print(self.host_addr_list, node_num)
        print(self.adj_addr_list, node_num)
        T_1 = threading.Thread(target=self.do_stuff)
        T_2 = threading.Thread(target=self.hello_process)
        T_1.start()
        T_2.start()
        print(f'Node Number {self.node_num} Threads started')
        T_1.join()
        T_2.join()

        ##Store Statistics
        print(f"Node Number {self.node_num} simulation finished")
        f = open(f'Node_{node_num}.json', 'w')
        stat = {"Adjacent Nodes History": list(self.adj_history_set)}

        stat.update({"Adjacent Nodes Data Send/Receive Number": self.num_data_send})

        tmp = []
        for i, adj_addr in enumerate(self.adj_addr_list):
            idx = self.correspond_idx(adj_addr, self.host_addr_list)
            tmp.append(idx)

        stat.update({"Last Adjacent Nodes": tmp})

        tmp = {}
        for host_idx, access_num in enumerate(self.access_count):
            if host_idx == self.node_num:
                continue
            tmp[f"Node_{host_idx}"] = access_num /self.total_time

        stat.update({'access_percentage': tmp})

        tmp = {}
        for i, adj_addr in enumerate(self.adj_addr_list):
            host_idx = self.correspond_idx(adj_addr, self.host_addr_list)
            connections = self.extract_connections(i)
            if not connections:
                continue
            tmp[f"Node_{host_idx} Connections"] = connections

        stat.update({'topology':tmp})

        json.dump(stat, f, indent=4)
        f.close()





def initial_net(num_nodes, num_conn, sim_time=100):
    end_sim = multiprocessing.Event() ## determine when simulation has to be stopped
    sleep_notif = [multiprocessing.Event() for _ in range(num_nodes)] ## detirmine which node should sleep for 20s
    obj_nodes_list = []
    addr_nodes_list = []
    for i in range(num_nodes):
        obj_nodes_list.append(node(num_conn=num_conn, end_sim=end_sim))
        addr_nodes_list.append(obj_nodes_list[i].hello_sock.getsockname())

    print('Making Obj finish')
    for i in range(num_nodes):
        obj_nodes_list[i].set_host_list(addr_nodes_list)
    #
    # print(addr_nodes_list)
    # print('**********************')
    # for i in range(num_nodes):
    #     print(obj_nodes_list[i].host_addr_list)
    #     print('**********************')

    print("Making Host lists finish")
    P = []
    for i in range(num_nodes):
        P.append(multiprocessing.Process(target=obj_nodes_list[i].run, args=(i, sleep_notif[i])))
        P[i].start()

    for i in range(int(sim_time/10)):
        sleep_node_idx = random.randint(0, num_nodes-1)
        sleep_notif[sleep_node_idx].set()
        time.sleep(10)
    end_sim.set()
    for i in range(num_nodes):
        P[i].join()




if __name__ == "__main__":
    if len(sys.argv) == 3:
        num_nodes = sys.argv[1]
        num_conn = sys.argv[3]
    else:
        while(1):
            try:
                num_nodes = int(input("Number of Nodes(Recommended : 6) = "))
                num_conn = int(input("Number of Connections per Node(Recommended : 3) = "))
                sim_time = int(input("Simulation Time(Recommended : 500) = "))
                break
            except:
                print("Try again")
    print('start')
    initial_net(num_nodes=num_nodes, num_conn=num_conn, sim_time=sim_time)













#>>> moshkel dar zaman dare! dorost beshe.
#>>> report neveshte beshe.






