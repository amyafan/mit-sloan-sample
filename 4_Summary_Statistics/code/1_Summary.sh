#!/bin/bash

#SBATCH --job-name=summary_statistics
#SBATCH --output=log/1_summary_out.txt
#SBATCH --error=log/1_summary_err.txt
#SBATCH -p sched_mit_sloan_interactive
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=40G
#SBATCH --time=2:00:00 
#SBATCH --constraint="centos7"

xvfb-run -d python 1_summary.py