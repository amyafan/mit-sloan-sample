#!/bin/bash

#SBATCH --job-name=bal_change
#SBATCH --output=log/3_bal_change_out.txt
#SBATCH --error=log/3_bal_change_err.txt
#SBATCH -p sched_mit_sloan_interactive
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=60G
#SBATCH --time=8:00:00 
#SBATCH --constraint="centos7"

xvfb-run -d python 3_bal_change.py