#!/bin/bash

#SBATCH --job-name=mortgage_per_capita
#SBATCH --output=log/7_mortgage_per_capita_out.txt
#SBATCH --error=log/7_mortgage_per_capita_err.txt
#SBATCH -p sched_mit_sloan_interactive
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=40G
#SBATCH --time=2:00:00 
#SBATCH --constraint="centos7"

xvfb-run -d python 7_Mortgage_Per_Capita.py