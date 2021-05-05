import sys
import os
import threading
import socket
import time
import uuid
import struct
from copy import deepcopy

# https://bluesock.org/~willkg/dev/ansi.html
ANSI_RESET = "\u001B[0m"
ANSI_RED = "\u001B[31m"
ANSI_GREEN = "\u001B[32m"
ANSI_YELLOW = "\u001B[33m"
ANSI_BLUE = "\u001B[34m"

_NODE_UUID = str(uuid.uuid4())[:8]


# noinspection PyCompatibility
def print_yellow(msg):
    print(f"{ANSI_YELLOW}{msg}{ANSI_RESET}")


def print_blue(msg):
    # noinspection PyCompatibility
    print(f"{ANSI_BLUE}{msg}{ANSI_RESET}")


def print_red(msg):
    print(f"{ANSI_RED}{msg}{ANSI_RESET}")


def print_green(msg):
    print(f"{ANSI_GREEN}{msg}{ANSI_RESET}")


def get_broadcast_port():
    return 35498


def get_node_uuid():
    return _NODE_UUID


class NeighborInfo(object):
    def __init__(self, delay, last_timestamp, ip=None, tcp_port=None):
        # Ip and port are optional, if you want to store them.
        self.delay = delay
        self.last_timestamp = last_timestamp
        self.ip = ip
        self.tcp_port = tcp_port


############################################
#######  Y  O  U  R     C  O  D  E  ########
############################################


# Don't change any variable's name.
# Use this hashmap to store the information of your neighbor nodes.
neighbor_information = {}
map_sem = threading.Semaphore()

# Leave the server socket as global variable.
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # tcp
server.bind(("", 0))
curr_portnum = server.getsockname()[1]


# Leave broadcaster as a global variable.
broadcaster = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # udp
broadcaster.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
# Enable broadcasting mode
broadcaster.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
broadcaster.bind(("", get_broadcast_port()))


# Setup the UDP socket

def get_broadcast_message(node_uuid, tcp_port):
    return f'{node_uuid} ON {tcp_port}'


def popFromNeighbour(key):
    map_sem.acquire()
    neighbor_obj = neighbor_information.pop(key)
    map_sem.release()
    return neighbor_obj


def insertIntoNeighbour(other_uuid, neighbor_node):
    map_sem.acquire()
    neighbor_information[other_uuid] = neighbor_node
    map_sem.release()


def get_obj_time_stamp(uuid):
    map_sem.acquire()
    count = neighbor_information[uuid].last_timestamp
    map_sem.release()
    return count


def update_map():
    map_sem.acquire()
    print('running update map', len(neighbor_information))
    new_dict = deepcopy(neighbor_information)
    new_keys = new_dict.keys()
    map_sem.release()
    for key in new_keys:
        if time.time() - get_obj_time_stamp(key) < 10:
            continue
        neighbor_obj = popFromNeighbour(key)
        uuid, other_ip, other_port = key, neighbor_obj.ip, neighbor_obj.tcp_port
        exchange_thread = daemon_thread_builder(exchange_timestamps_thread, (uuid, other_ip, other_port))
        exchange_thread.start()
        # exchange_thread.join()
    # print(f'\nUpdated map length: {len(neighbor_information)}\n', neighbor_information, '\n\n')
    '''
     Un-comment the 2 previous lines to see the changes in the hash-map
     '''


def send_broadcast_thread():
    node_uuid = get_node_uuid()
    print('my uuid: ', node_uuid, ' my ip: ', curr_portnum)
    counter = 0
    while True:
        message = get_broadcast_message(node_uuid, curr_portnum)
        message_bytes = bytes(message, 'utf-8')
        portnum = get_broadcast_port()
        broadcaster.sendto(message_bytes, ("255.255.255.255", portnum))  # ''socket.INADDR_BROADCAST '<broadcast>'
        counter += 1
        if counter % 10 == 0:
            update_map()
        time.sleep(1)  # Leave as is.


def handlebroadcast(packet):
    packet = packet.decode('utf-8')
    recuuid, _, portnum = packet.split()
    portnum = int(portnum)
    return recuuid, portnum


def receive_broadcast_thread():
    """
    Receive broadcasts from other nodes,
    launches a thread to connect to new nodes
    and exchange timestamps.
    """
    myuuid = get_node_uuid()
    while True:
        # TODO: write logic for receiving broadcasts.
        data, (ip, port) = broadcaster.recvfrom(4096)
        uuid, port = handlebroadcast(data)

        if uuid == myuuid:
            continue

        map_sem.acquire()
        new_dict = deepcopy(neighbor_information)
        new_keys = new_dict.keys()
        map_sem.release()

        if uuid in new_keys:
            continue

        exchanging_thread = daemon_thread_builder(exchange_timestamps_thread, (uuid, ip, port))
        exchanging_thread.start()
        print_blue(f"RECV: {uuid} FROM: {ip}:{port}")


def tcp_server_thread():
    """
    Accept connections from other nodes and send them
    this node's timestamp once they connect.
    """
    while True:
        server.listen(4096)
        conn, addr = server.accept()
        with conn:
            print_green(f"Connected by {addr} ")
            now = time.time()
            our_timestamp = struct.pack("!d", now)
            conn.sendall(our_timestamp)


def exchange_timestamps_thread(other_uuid: str, other_ip: str, other_tcp_port: int):
    """
    Open a connection to the other_ip, other_tcp_port
    and do the steps to exchange timestamps.t

    Then update the neighbor_info map using other node's UUID.
    """

    print_yellow(f"ATTEMPTING TO CONNECT TO {other_uuid}")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:

        # send our timestamp
        try:
            s.connect((other_ip, other_tcp_port))
            now = time.time()
            our_timestamp = struct.pack("!d", now)

            s.sendall(our_timestamp)
            data = s.recv(1024)
        except ConnectionRefusedError:
            return
        except ConnectionResetError:
            return

        # receive timestamp
        last_timestamp = struct.unpack("!d", data)[0]
        print_yellow(f'received from {other_ip} {other_tcp_port} time stamp  {last_timestamp}')
        delay = time.time() - last_timestamp
        neighbor_node = NeighborInfo(delay, last_timestamp, ip=other_ip, tcp_port=other_tcp_port)
        print('inserting into neighbour in exchange')
        insertIntoNeighbour(other_uuid, neighbor_node)


def daemon_thread_builder(target, args=()) -> threading.Thread:
    """
    Use this function to make threads. Leave as is.
    """
    th = threading.Thread(target=target, args=args)
    th.setDaemon(True)
    return th


def entrypoint():
    tcp_listener = daemon_thread_builder(tcp_server_thread)
    broadcast_sender = daemon_thread_builder(send_broadcast_thread)
    broadcast_reciever = daemon_thread_builder(receive_broadcast_thread)
    tcp_listener.start()
    broadcast_sender.start()
    broadcast_reciever.start()
    broadcast_sender.join()

    pass


############################################
############################################


def main():
    """
    Leave as is.
    """
    print("*" * 50)
    print_red("To terminate this program use: CTRL+C")
    print_red("If the program blocks/throws, you have to terminate it manually.")
    print_green(f"NODE UUID: {get_node_uuid()}")
    print("*" * 50)
    time.sleep(2)  # Wait a little bit.
    entrypoint()


if __name__ == "__main__":
    main()
