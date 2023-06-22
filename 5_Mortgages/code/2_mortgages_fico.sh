#!/bin/bash
#SBATCH --job-name=mortgages_fico
#SBATCH --output=log/2_mortgages_fico_out.txt
#SBATCH --error=log/2_mortgages_fico_err.txt
#SBATCH -p sched_mit_sloan_interactive
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=20000
#SBATCH --time=3:00:00 
#SBATCH --constraint="centos7"

xvfb-run -d python 2_mortgages_fico.py