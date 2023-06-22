#!/bin/bash

#SBATCH --job-name=student_debt_age
#SBATCH --output=log/5_student_debt_out.txt
#SBATCH --error=log/5_studetn_debt_err.txt
#SBATCH -p sched_mit_sloan_interactive
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=40G
#SBATCH --time=2:00:00 
#SBATCH --constraint="centos7"

xvfb-run -d python 5_Student_Debt_Age.py