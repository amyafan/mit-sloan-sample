#!/bin/bash

#SBATCH --job-name=new_consumers
#SBATCH --output=log/4_new_consumers_out.txt
#SBATCH --error=log/4_new_consumers_err.txt
#SBATCH -p sched_any_quicktest
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=10G
#SBATCH --time=0:15:00 
#SBATCH --constraint="centos7"

xvfb-run -d python 4_New_Consumers.py