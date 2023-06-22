#!/bin/bash

#SBATCH --job-name=panel_construction
#SBATCH --output=log/2_panel_construction_%a_out.txt
#SBATCH --error=log/2_panel_construction_%a_err.txt
#SBATCH -p sched_mit_sloan_batch
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=32G
#SBATCH --time=2:00:00 
#SBATCH --constraint="centos7"
#SBATCH --mail-user=afan@mit.edu
#SBATCH --mail-type=TIME_LIMIT

#SBATCH --array=1-195

python3 2_Panel_Construction.py ${SLURM_ARRAY_TASK_ID}

