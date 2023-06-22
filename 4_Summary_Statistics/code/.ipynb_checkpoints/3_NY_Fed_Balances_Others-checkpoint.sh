#!/bin/bash

#SBATCH --job-name=ny_fed_balances_others
#SBATCH --output=log/3_ny_fed_others_out.txt
#SBATCH --error=log/3_ny_fed_others_err.txt
#SBATCH -p sched_mit_sloan_interactive
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=40G
#SBATCH --time=3:00:00 
#SBATCH --constraint="centos7"

xvfb-run -d python 3_NY_Fed_Balances_Others.py