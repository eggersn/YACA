from http import server
import enquiries
import os
import sys
import re
import math

import multiprocessing
from src.components.server.server import Server
from src.core.utils.configuration import Configuration

processes = {}


def clear():
    os.system("clear")


def print_banner():
    columns, rows = os.get_terminal_size(0)
    print("*" * columns)
    print("__   _____  _____   ___        _                            _               ".center(columns, " "))
    print("\ \ / / _ \/  __ \ / _ \      | |                          | |              ".center(columns, " "))
    print(" \ V / /_\ \ /  \// /_\ \     | |     __ _ _   _ _ __   ___| |__   ___ _ __ ".center(columns, " "))
    print("  \ /|  _  | |    |  _  |     | |    / _` | | | | '_ \ / __| '_ \ / _ \ '__|".center(columns, " "))
    print("  | || | | | \__/\| | | |     | |___| (_| | |_| | | | | (__| | | |  __/ |   ".center(columns, " "))
    print("  \_/\_| |_/\____/\_| |_/     \_____/\__,_|\__,_|_| |_|\___|_| |_|\___|_|   ".center(columns, " "))
    print()
    print("*" * columns)


def print_menu_banner(menu_text):
    clear()
    columns, rows = os.get_terminal_size(0)
    print_banner()
    print(menu_text.center(columns))
    print("*" * columns)


def main_menu():
    exiting = False

    while not exiting:
        print_menu_banner("Main Menu")

        options = [
            "1. Spawn Processes",
            "2. Kill Processes",
            "3. Run Tests",
            "4. Exit (Ctrl+C)",
        ]
        choice = enquiries.choose("Choose one of these options: ", options)

        if choice == options[0]:
            spawn_processes_menu()
        elif choice == options[1]:
            kill_processes_menu()
        elif choice == options[3]:
            return


def kill_processes_menu():
    exiting = False

    while not exiting:
        print_menu_banner("Kill Processes")

        options = []
        counter = 1
        for component in processes:
            options.append(str(counter) + ". Kill " + component)
            counter += 1
        options.append(str(counter) + ". Return to Main Menu")
        choice = enquiries.choose("Choose one of these options: ", options)

        if choice == options[-1]:
            return

        counter = 1
        for component in processes:
            if choice == str(counter) + ". Kill " + component:
                kill_processes_component_menu(component)
            counter += 1


def kill_processes_component_menu(component):
    print_menu_banner("Kill Processes (Selection: " + component + ") - Suggestion: Upper limit for Byzantine faults: " + str(math.ceil(len(processes[component])/4)-1))

    options = []
    counter = 1
    for p in processes[component]:
        options.append(str(counter) + str(p))
        counter += 1
    options.append(str(counter) + ". Return")
    choice = enquiries.choose("Choose multiple of these options (space to select): ", options, multi=True)

    if options[-1] in choice:
        return

    terminating_procs = []
    counter = 1
    for p in processes[component]:
        if str(counter) + str(p) in choice:
            p.terminate()
            terminating_procs.append(p)
        counter += 1

    while len(terminating_procs) > 0:
        for p in terminating_procs:
            if not p.is_alive():
                p.join()
                terminating_procs.remove(p)
                processes[component].remove(p)


def spawn_processes_menu():
    while True:
        print_menu_banner("Spawn Processes")
        options = [
            "1. Launch Server",
            "2. Launch Client",
            "3. Return to Main Menu",
        ]
        choice = enquiries.choose("Choose one of these options: ", options)

        if choice == options[-1]:
            return False

        print("Number of Instances: ", end="")
        instances = int(input())

        if choice == options[0]:
            for i in range(instances):
                p = multiprocessing.Process(target=launch_server)
                p.start()
                processes["server"].append(p)


def launch_server(initial=False, i=0):
    sys.stdout = open("logs/server-" + str(os.getpid()) + ".out", "a", buffering=1)
    sys.stderr = open("logs/server-" + str(os.getpid()) + ".out", "a", buffering=1)
    server = Server(initial, i, verbose=True)
    server.start()


def main():
    for f in os.listdir("logs/"):
        os.remove(os.path.join("logs/", f))

    processes["server"] = []

    if len(sys.argv) > 1 and sys.argv[1] == '-i':
        config = Configuration()
        for i in range(config.data["initial"]["instances"]):
            p = multiprocessing.Process(
                target=launch_server,
                args=(
                    True,
                    i,
                ),
            )
            p.start()
            processes["server"].append(p)

    main_menu()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("exiting...")
    finally:
        for component in processes:
            for p in processes[component]:
                p.terminate()
        print("Waiting for processes to terminate...")
        while sum([len(processes[component]) for component in processes]) > 0:
            for component in processes:
                for p in processes[component]:
                    if not p.is_alive():
                        p.join()
                        processes[component].remove(p)
