# ChiaSimplePlotter
A simple python plotter manager for the latest GPU-enabled plotters.
Currently, it only works on Linux, but it just takes several minutes to modify it to support Windows (if you know Python).

## Usage
Open the config.ini in any text editor and modify the configurations based on the comments. The constants you must change are:
FARMER_KEY, POOL_CONTRACT, PLOT_CACHE_PATH, FARMS

The rest constants are optional. I recommend you copy the plotter binary to this root folder, otherwise you also need to change PLOTTER_PATH

## Features

- Respawn the plotter automatically
- Support any type of plotter, Bladebit, Gigahorse, etc. (Modify the plot command in plotter.py)
- Replace your old plots by their creation time (Enable Replot Mode)
- Modify/Upgrade the control program without halting plotting/distribution
- Distribute plots in SSD to HDD based on your bandwidth (You need to config it)
