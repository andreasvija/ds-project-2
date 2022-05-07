from _thread import start_new_thread
from random import random
from statistics import mode
from sys import argv
from time import sleep

import rpyc

STATE_FAULTY = 'F'
STATE_NON_FAULTY = 'NF'

PREFIX_CLIENT = 'CLIENT'
PREFIX_PRIMARY = 'PRIMARY'
PREFIX_SECONDARY = 'SECONDARY'

ORDER_ATTACK = 'attack'
ORDER_RETREAT = 'retreat'

PROCESS_IDS = set()


def get_port_from_id(id):
    return 1234 + id


def listen_server_generator(given_handler):
    class ListenServer(rpyc.Service):
        handler = given_handler

        def exposed_message(self, incoming_message):
            self.handler(incoming_message)

    return ListenServer


def send_message(id, message):
    conn = rpyc.connect('localhost', get_port_from_id(id), config={'sync_request_timeout': 300})
    conn.root.message(message)
    conn.close()


class Process:

    def __init__(self, id):
        self.id = id
        self.state = STATE_NON_FAULTY
        self.is_primary = False
        self.listen_server = rpyc.utils.server.ThreadedServer(
            listen_server_generator(self.handle_request),
            port=get_port_from_id(self.id)
        )
        self.orders = []

    def start(self):
        start_new_thread(self.listen, ())

    def listen(self):
        self.listen_server.start()

    def stop(self):
        self.listen_server.close()

    def send_orders(self, order):
        for id in PROCESS_IDS:
            if id != self.id:
                self.send_order(id, order)

    def send_order(self, id, order):
        prefix = PREFIX_PRIMARY if self.is_primary else PREFIX_SECONDARY

        if self.state == STATE_FAULTY:
            if random() < 0.5:
                order = ORDER_RETREAT
            else:
                order = ORDER_ATTACK

        send_message(id, f'{prefix} {order}')

    def handle_request(self, incoming_message):
        prefix = incoming_message.split(' ')[0]
        order = incoming_message.split(' ')[1]

        if prefix == PREFIX_CLIENT:
            assert self.is_primary
            self.orders.append(order)
            self.send_orders(order)

        elif self.is_primary:
            return

        elif prefix == PREFIX_PRIMARY:
            self.orders.append(order)
            self.send_orders(order)

        elif prefix == PREFIX_SECONDARY:
            self.orders.append(order)

        else:
            assert False, 'Unknown prefix'


if __name__ == '__main__':
    n = int(argv[1])
    assert n > 0

    processes = []
    PROCESS_IDS = set()

    for id in range(1, n + 1):
        process = Process(id)
        process.start()
        processes.append(process)
        PROCESS_IDS.add(id)

        if id == 1:
            process.is_primary = True

    print('Commands: quit, actual-order o, g-state, g-state i s, g-kill i, g-add n')

    while True:
        user_input = input().strip().replace('  ', ' ')

        if user_input == 'quit':
            break

        elif user_input.split(' ')[0] == 'actual-order':
            order = user_input.split(' ')[1]
            assert order == ORDER_ATTACK or order == ORDER_RETREAT

            primary_id = None
            for process in processes:
                if process.is_primary:
                    primary_id = process.id
                    break
            assert primary_id is not None

            send_message(primary_id, f'{PREFIX_CLIENT} {order}')
            sleep(1)

            process_count = len(processes)
            majority_size = process_count // 2 + 1
            faulty_count = sum([1 for process in processes if process.state == STATE_FAULTY])
            enough_processes = process_count >= 3 * faulty_count + 1

            if process_count < 2:
                for process in processes:
                    print(f'G{process.id}, '
                          f'{"primary" if process.is_primary else "secondary"}, '
                          f'majority={order} '
                          f'state={process.state}')
                print('Execute order: cannot be determined – not enough generals in the system! '
                      'Less than 2 generals means that failures can never be detected.')
                continue

            majorities = []
            for process in processes:
                majority = mode(process.orders)
                majority_support = sum(1 for order in process.orders if order == majority)

                process.orders = []
                majorities.append(majority)

                print(f'G{process.id}, '
                      f'{"primary" if process.is_primary else "secondary"}, '
                      f'majority={majority} '
                      f'state={process.state}')
            final_majority = mode(majorities)

            if not enough_processes:
                print('Execute order: cannot be determined – not enough generals in the system! '
                      f'{faulty_count} faulty nodes in the system - '
                      f'at least {majority_size} out of {process_count} quorum not consistent.')
                continue

            print(f'Execute order: {final_majority}! '
                  f'{faulty_count} faulty nodes in the system – '
                  f'at least {majority_size} out of {process_count} quorum suggest {final_majority}')

        elif user_input == 'g-state':
            for process in processes:
                print(f'G{process.id}, '
                      f'{"primary" if process.is_primary else "secondary"}, '
                      f'state={process.state}')

        elif user_input.split(' ')[0] == 'g-state':
            id = int(user_input.split(' ')[1])
            faulty = user_input.split(' ')[2] == 'faulty'
            for process in processes:
                if process.id == id:
                    process.state = STATE_FAULTY if faulty else STATE_NON_FAULTY
                    break

        elif user_input.split(' ')[0] == 'g-kill':
            id = int(user_input.split(' ')[1])
            PROCESS_IDS.remove(id)

            for i in range(len(processes)):
                if processes[i].id == id:
                    was_primary = processes[i].is_primary
                    processes[i].stop()

                    processes_a = processes[:i]
                    processes_b = []
                    if i != len(processes) - 1:
                        processes_b = processes[i + 1:-1]
                    processes = processes_a + processes_b

                    if was_primary and len(processes) > 0:
                        processes[0].is_primary = True
                    break

        elif user_input.split(' ')[0] == 'g-add':
            k = int(user_input.split(' ')[1])
            start = max(PROCESS_IDS) + 1

            for id in range(start, start + k):
                process = Process(id)
                process.start()
                processes.append(process)
                PROCESS_IDS.add(id)

        else:
            print('unknown input')
