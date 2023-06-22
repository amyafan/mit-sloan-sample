#!/bin/bash

#SBATCH --job-name=ny_fed_balances_accounts
#SBATCH --output=log/2_ny_fed_out.txt
#SBATCH --error=log/2_ny_fed_err.txt
#SBATCH -p sched_mit_sloan_interactive
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=80G
#SBATCH --time=0:30:00 
#SBATCH --constraint="centos7"

xvfb-run -d python 2_NY_Fed_Balances_Accounts.py