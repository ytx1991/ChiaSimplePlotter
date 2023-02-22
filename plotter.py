import configparser
import json
import logging
import os
import subprocess
import time
from logging.handlers import TimedRotatingFileHandler

import psutil

global logger
config = configparser.ConfigParser()
config.read(r'config.ini')
# Scan period
SCAN_SECOND = int(config.get("General", "SCAN_SECOND"))
# Your Farmer Public Key
FARMER_KEY = config.get("Plotting", "FARMER_KEY")
# Your Pool Contract
POOL_CONTRACT = config.get("Plotting", "POOL_CONTRACT")
# Path of your plotter binary
# Valid Bladebit path needs to contain "bladebit"
PLOTTER_PATH = config.get("Plotting", "PLOTTER_PATH")
# Your SSD cache for plots, for Gigahorse, it has to end with /
PLOT_CACHE_PATH = config.get("Distributing", "PLOT_CACHE_PATH")
# No new plotting when memory utilization is higher than this number
REQUIRED_MEM_PERCENT = int(config.get("Plotting", "REQUIRED_MEM_PERCENT"))
# No new plotting when cache SSD free space is lower than this number
REQUIRED_CACHE_GB = int(config.get("Plotting", "REQUIRED_CACHE_GB"))
# Compression level
COMPRESSION_LEVEL = int(config.get("Plotting", "COMPRESSION_LEVEL"))
# Concurrent copy how many plots
MAX_COPY_THREAD = int(config.get("Distributing", "MAX_COPY_THREAD"))
# Prevent continuously spawn plotting
COOLDOWN_CYCLE = int(config.get("General", "COOLDOWN_CYCLE"))
# If you want to replace old plots when the disk is full
REPLOT_MODE = bool(config.get("General", "REPLOT_MODE"))
REPLACE_DDL = int(config.get("Distributing", "REPLACE_DDL"))
FARM_SPARE_GB = int(config.get("Distributing", "FARM_SPARE_GB"))
# Destination of HDDs
FARMS = json.loads(config.get("Distributing", "FARMS"))

BLADEBIT_COMMAND = f"{PLOTTER_PATH} -f {FARMER_KEY} -n 0 -t 20 -c {POOL_CONTRACT} --compress {COMPRESSION_LEVEL} cudaplot {PLOT_CACHE_PATH} "
GIGAHORSE_COMMAND = f"{PLOTTER_PATH} -f {FARMER_KEY} -n -1 -c {POOL_CONTRACT} -C {COMPRESSION_LEVEL} -t {PLOT_CACHE_PATH} "

plot_in_transfer = set([])
plot_in_pending = set([])
farm_in_transfer = set([])
last_plot_time = 0
spawn_plotter = True
plot_in_deletion = set([])

last_plot_cycle = COOLDOWN_CYCLE

def main():
    global last_plot_cycle
    while True:
        try:
            # Check if need to trigger a new plotting
            mem_usage = psutil.virtual_memory()[2]
            cache_free = psutil.disk_usage(PLOT_CACHE_PATH)[2]/1024/1024/1024
            logger.info(f"Start scanning, current memory usage: {mem_usage}%, cache free GB:{cache_free}, last plotting triggered at {last_plot_cycle} cycle ago.")
            if mem_usage <= REQUIRED_MEM_PERCENT and cache_free >= REQUIRED_CACHE_GB and spawn_plotter and last_plot_cycle > COOLDOWN_CYCLE:
                logger.info("Has enough memory and cache space, spawn the plotter ...")
                # Change this based on your OS and plotter
                subprocess.Popen(
                    [f"{BLADEBIT_COMMAND if PLOTTER_PATH.find('bladebit')>=0 else GIGAHORSE_COMMAND} > tmp.txt && rm tmp.txt"],
                    shell=True)
                last_plot_cycle = 0

            last_plot_cycle += 1
            # Update in transfer plots
            update_in_transfer()
            # Keep farm spare space
            if REPLOT_MODE:
                spare_farms = 0
                for farm in FARMS:
                    farm_free = psutil.disk_usage(farm)[2]
                    if farm_free > FARM_SPARE_GB * 1024 * 1024 * 1024:
                        spare_farms += 1
                if spare_farms >= MAX_COPY_THREAD:
                    # We have enough spare space
                    logger.info(f"Need {MAX_COPY_THREAD}, {spare_farms} farms available.")
                else:
                    clean_farm(MAX_COPY_THREAD - spare_farms)
            # Check if there is any plot need to move
            move_plots()
        except Exception as e:
            logger.exception("Error")
        finally:
            time.sleep(SCAN_SECOND)


def clean_farm(need_farms: int):
    cleaned_farms = 0
    for farm in FARMS:
        # Need to remove old plots
        remove_plots = []
        remove_size = psutil.disk_usage(farm)[2]
        for plot in os.listdir(farm):
            creation_date = os.path.getctime(f"{farm}/{plot}")
            if creation_date < REPLACE_DDL:
                remove_plots.append(f"{farm}/{plot}")
                plot_size = os.path.getsize(f"{farm}/{plot}")
                remove_size += plot_size
            if remove_size > FARM_SPARE_GB * 1024 * 1024 * 1024:
                for rm_plot in remove_plots:
                    if rm_plot not in plot_in_deletion:
                        subprocess.Popen([f"rm {rm_plot}"], shell=True)
                        logger.info(f"Removing {rm_plot} for new plot ...")
                        plot_in_deletion.add(rm_plot)

                cleaned_farms += 1
                break
        if cleaned_farms >= need_farms:
            break
    if cleaned_farms < need_farms:
        logger.warning(f"Cannot clean up {need_farms} farms, all farms will full soon.")


def move_plots():
    for file in os.listdir(PLOT_CACHE_PATH):
        if file.endswith('.plot') and f"{PLOT_CACHE_PATH}/{file}" not in plot_in_transfer and len(
                plot_in_transfer) < MAX_COPY_THREAD:
            # Check which farm has space
            find_disk = False
            file_size = os.path.getsize(f"{PLOT_CACHE_PATH}/{file}")
            for farm in FARMS:
                farm_free = psutil.disk_usage(farm)[2]
                if farm not in farm_in_transfer and farm_free > file_size and len(farm_in_transfer) < MAX_COPY_THREAD:
                    plot_in_transfer.add(f"{PLOT_CACHE_PATH}/{file}")
                    farm_in_transfer.add(farm)
                    logger.info(f"Start moving plot {file} to {farm} ...")
                    # Modify this command if you want to copy remotely
                    subprocess.Popen([f"cp {PLOT_CACHE_PATH}/{file} {farm} && mv {PLOT_CACHE_PATH}/{file} {PLOT_CACHE_PATH}/{file}.delete && rm {PLOT_CACHE_PATH}/{file}.delete"],
                                     shell=True)
                    find_disk = True
                    break
            if not find_disk:
                logger.warning(f"Cannot find farm for {file}, please check disk usage or enable replot mode.")


def update_in_transfer():
    global last_plot_time
    global spawn_plotter
    processes = psutil.process_iter()
    plot_in_transfer.clear()
    farm_in_transfer.clear()
    plot_in_pending.clear()
    spawn_plotter = True
    for process in processes:
        try:
            command = process.cmdline()
            if len(command) >= 3 and command[0] == "cp" and command[1].find(PLOT_CACHE_PATH) >= 0 and command[1].find(".plot") >= 0:
                plot_in_transfer.add(command[1])
                farm_in_transfer.add(command[2])
            if len(command) > 0 and command[0].find(PLOTTER_PATH) >= 0:
                spawn_plotter = False
        except Exception:
            pass

    for file in os.listdir(PLOT_CACHE_PATH):
        if file.endswith('.plot'):
            plot_in_pending.add(file)
            create_time = os.path.getctime(f"{PLOT_CACHE_PATH}/{file}")
            if create_time > last_plot_time:
                logger.info(f"Found new plot {file}, created at {create_time}.")
                last_plot_time = create_time

    logger.info(f"Detected {len(plot_in_transfer)} plots in transfer, {len(plot_in_pending) - len(plot_in_transfer)} plots is pending for move.")


if __name__ == "__main__":
    if os.path.exists("tmp.txt"):
        os.remove("tmp.txt")
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)
    handler = TimedRotatingFileHandler("plotter.log",
                                   when="d",
                                   interval=1,
                                   backupCount=7)
    formatter = logging.Formatter("%(asctime)s [%(process)d] [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    update_in_transfer()
    main()