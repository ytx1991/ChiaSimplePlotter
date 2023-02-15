import logging
import os
import subprocess
import time
from logging.handlers import TimedRotatingFileHandler

import psutil

global logger

# Scan period
SCAN_SECOND = 10
# Your Farmer Public Key
FARMER_KEY = "b64bf1a9dd9378da07fe5c656a9f609838f14d13173805cb25940cce609866d367d0f51b08e5b3fcc7795acdd620a581"
# Your Pool Contract
POOL_CONTRACT = "xch1mw5mhk3jukd6ds0cttzptqqzkqjujaepwhn94h4yukdu33jrrlmqkvap63"
# Path of your plotter binary
PLOTTER_PATH = "./bladebit_cuda"
# Your SSD cache for plots
PLOT_CACHE_PATH = r"/mnt/plot"
# No new plotting when memory utilization is higher than this number
REQUIRED_MEM_PERCENT = 50
# No new plotting when cache SSD free space is lower than this number
REQUIRED_CACHE_GB = 105
# Compression level
COMPRESSION_LEVEL = 0
# Concurrent copy how many plots
MAX_COPY_THREAD = 5
# If you want to replace old plots when the disk is full
REPLOT_MODE = False
# Destination of HDD
FARMS = ["/mnt/farm1", "/mnt/farm2", "/mnt/farm3", "/mnt/farm4", "/mnt/farm5", "/mnt/farm6", "/mnt/farm7", "/mnt/farm8", "/mnt/farm9",
         "/mnt/farm10", "/mnt/farm11", "/mnt/farm12", "/mnt/farm13", "/mnt/farm14", "/mnt/farm15", "/mnt/farm16", "/mnt/farm17", "/mnt/farm18", "/mnt/farm19",
         "/mnt/farm20", "/mnt/farm21", "/mnt/farm22", "/mnt/farm23", "/mnt/farm24", "/mnt/farm25", "/mnt/farm26", "/mnt/farm27", "/mnt/farm28", "/mnt/farm29",
         "/mnt/farm30", "/mnt/farm31", "/mnt/farm32", "/mnt/farm33", "/mnt/farm34", "/mnt/farm35", "/mnt/farm36", "/mnt/farm37", "/mnt/farm38", "/mnt/farm39",
         "/mnt/farm40", "/mnt/farm41", "/mnt/farm42"]
plot_in_transfer = set([])
plot_in_pending = set([])
farm_in_transfer = set([])
last_plot_time = 0
spawn_plotter = True

def main():
    global last_plot_cycle
    while True:
        try:
            # Check if need to trigger a new plotting
            mem_usage = psutil.virtual_memory()[2]
            cache_free = psutil.disk_usage(PLOT_CACHE_PATH)[2]/1024/1024/1024
            logger.info(f"Start scanning, current memory usage: {mem_usage}%, cache free GB:{cache_free}, last plot created at {last_plot_time}.")
            if mem_usage <= REQUIRED_MEM_PERCENT and cache_free >= REQUIRED_CACHE_GB and spawn_plotter:
                logger.info("Has enough memory and cache space, spawn the plotter ...")
                # Change this based on your OS and plotter
                subprocess.Popen(
                    [f"{PLOTTER_PATH} -f {FARMER_KEY} -n 0 -t 20 -c {POOL_CONTRACT} --compress {COMPRESSION_LEVEL} cudaplot {PLOT_CACHE_PATH} > tmp.txt && rm tmp.txt"],
                    shell=True)
                last_plot_cycle = 0

            last_plot_cycle += 1
            # Update in transfer plots
            update_in_transfer()
            # Check if there is any plot need to move
            for file in os.listdir(PLOT_CACHE_PATH):
                if file.endswith('.plot') and f"{PLOT_CACHE_PATH}/{file}" not in plot_in_transfer and len(plot_in_transfer) < MAX_COPY_THREAD:
                    # Check which farm has space
                    file_size = os.path.getsize(f"{PLOT_CACHE_PATH}/{file}")
                    for farm in FARMS:
                        farm_free = psutil.disk_usage(farm)[2]
                        if farm not in farm_in_transfer and farm_free > file_size and len(farm_in_transfer) < MAX_COPY_THREAD:
                            plot_in_transfer.add(f"{PLOT_CACHE_PATH}/{file}")
                            farm_in_transfer.add(farm)
                            logger.info(f"Start moving plot {file} to {farm} ...")
                            # Modify this command if you want to copy remotely
                            subprocess.Popen([f"cp {PLOT_CACHE_PATH}/{file} {farm} && mv {PLOT_CACHE_PATH}/{file} {PLOT_CACHE_PATH}/{file}.delete && rm {PLOT_CACHE_PATH}/{file}.delete"], shell=True)
                            break
                        else:
                            continue
        except Exception as e:
            logger.exception("Error")
        finally:
            time.sleep(SCAN_SECOND)

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