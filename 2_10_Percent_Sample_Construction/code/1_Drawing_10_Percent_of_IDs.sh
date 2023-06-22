#!/bin/bash

#SBATCH --job-name=drawing_10_percent
#SBATCH --output=log/1_Drawing_10_Percent_of_IDs_out.txt
#SBATCH --error=log/1_Drawing_10_Percent_of_IDs_err.txt
#SBATCH -p sched_mit_sloan_interactive
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=16G
#SBATCH --time=1:00:00 
#SBATCH --constraint="centos7"

xvfb-run -d python 1_Drawing_10_Percent_of_IDs.py