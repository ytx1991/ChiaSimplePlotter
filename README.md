# ChiaSimplePlotter
A simple python plotter manager for the latest GPU enabled plotters.
Currently, it only works on Linux / Bladebit, but it just takes several minutes to modify it to support Windows/Gigahorse (if you know Python).

## Usage
Open the plotter.py in any text editor. The constants you must change are:
FARMER_KEY, POOL_CONTRACT, PLOT_CACHE_PATH, FARMS

The rest constants are optional. I recommend you copy the plotter binary to this root folder, otherwise you also need to change PLOTTER_PATH

## Features

- Respawn the plotter automatically
- Distribute plots in SSD to HDD based on your bandwidth
