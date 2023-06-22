#!/bin/bash

#SBATCH --job-name=mort_entr_exit
#SBATCH --output=log/1_mort_entr_exit_out.txt
#SBATCH --error=log/1_mort_entr_exit_err.txt
#SBATCH -p sched_mit_sloan_interactive
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=20G
#SBATCH --time=0:30:00 
#SBATCH --constraint="centos7"

xvfb-run -d python 1_mort_entr_exit.py