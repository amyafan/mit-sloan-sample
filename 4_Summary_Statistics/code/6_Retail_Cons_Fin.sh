#!/bin/bash

#SBATCH --job-name=retail_cons_fin
#SBATCH --output=log/6_retail_cons_fin_out.txt
#SBATCH --error=log/6_retail_cons_fin_err.txt
#SBATCH -p sched_mit_sloan_interactive
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=200G
#SBATCH --time=2:00:00 
#SBATCH --constraint="centos7"

xvfb-run -d python 6_Retail_Cons_Fin.py