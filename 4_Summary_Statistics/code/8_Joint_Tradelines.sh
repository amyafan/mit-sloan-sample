#!/bin/bash

#SBATCH --job-name=joint_tradelines
#SBATCH --output=log/8_joint_tradelines_out.txt
#SBATCH --error=log/8_joint_tradelines_err.txt
#SBATCH -p sched_mit_sloan_interactive
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=40G
#SBATCH --time=2:00:00 
#SBATCH --constraint="centos7"

xvfb-run -d python 8_Joint_Tradelines.py