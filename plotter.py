import logging
import os
import subprocess
import time
from logging.handlers import TimedRotatingFileHandler

import psutil

global logger

SCAN_SECOND = 5
FARMER_KEY = "b64bf1a9dd9378da07fe5c656a9f609838f14d13173805cb25940cce609866d367d0f51b08e5b3fcc7795acdd620a581"
POOL_CONTRACT = "xch1mw5mhk3jukd6ds0cttzptqqzkqjujaepwhn94h4yukdu33jrrlmqkvap63"
PLOTTER_PATH = "./bladebit_cuda"
PLOT_CACHE_PATH = r"/mnt/plot"
REQUIRED_MEM_PERCENT = 50
REQUIRED_CACHE_GB = 105
COMPRESSION_LEVEL = 0
MAX_COPY_THREAD = 5
REPLOT_MODE = False
FARMS = ["/mnt/farm1", "/mnt/farm2", "/mnt/farm3", "/mnt/farm4", "/mnt/farm5", "/mnt/farm6", "/mnt/farm7", "/mnt/farm8", "/mnt/farm9",
         "/mnt/farm10", "/mnt/farm11", "/mnt/farm12", "/mnt/farm13", "/mnt/farm14", "/mnt/farm15", "/mnt/farm16", "/mnt/farm17", "/mnt/farm18", "/mnt/farm19",
         "/mnt/farm20", "/mnt/farm21", "/mnt/farm22", "/mnt/farm23", "/mnt/farm24", "/mnt/farm25", "/mnt/farm26", "/mnt/farm27", "/mnt/farm28", "/mnt/farm29",
         "/mnt/farm30", "/mnt/farm31", "/mnt/farm32", "/mnt/farm33", "/mnt/farm34", "/mnt/farm35", "/mnt/farm36", "/mnt/farm37", "/mnt/farm38", "/mnt/farm39",
         "/mnt/farm40", "/mnt/farm41", "/mnt/farm42"]
plot_in_transfer = set([])
farm_in_transfer = set([])


def main():
    while True:
        try:
            # Check if need to trigger a new plotting
            mem_usage = psutil.virtual_memory()[2]
            cache_free = psutil.disk_usage(PLOT_CACHE_PATH)[2]/1024/1024/1024
            logger.info(f"Start scanning, current memory usage: {mem_usage}%, cache free GB:{cache_free}")
            if mem_usage <= REQUIRED_MEM_PERCENT and cache_free >= REQUIRED_CACHE_GB and not os.path.exists("tmp.txt"):
                logger.info("Has enough memory and cache space, trigger a new plotting ...")
                # Change this based on your OS and plotter
                subprocess.Popen(
                    [f"{PLOTTER_PATH} -f {FARMER_KEY} -c {POOL_CONTRACT} --compress {COMPRESSION_LEVEL} cudaplot {PLOT_CACHE_PATH} > tmp.txt && rm tmp.txt"], shell=True)
            else:
                logger.info("Doesn't has enough memory or cache space, wait for next scan ...")

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
    processes = psutil.process_iter()
    plot_in_transfer.clear()
    farm_in_transfer.clear()
    for process in processes:
        try:
            command = process.cmdline()
            if len(command) >= 3 and command[0] == "cp" and command[1].find(PLOT_CACHE_PATH) >= 0 and command[1].find(".plot") >= 0:
                plot_in_transfer.add(command[1])
                farm_in_transfer.add(command[2])
        except Exception:
            pass
    logger.info(f"Detected {len(plot_in_transfer)} plots in transfer.")


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