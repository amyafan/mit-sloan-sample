# Code sample

This is a sample of the code that I wrote as an RA at MIT Sloan. For months, I worked with data from a credit reporting agency trying to learn more about trends in auto loans, mortgages, and student loan debt. 

This was a longitudinal dataset that followed the credit reports of 10% sample of US consumers for over 15 years. The main thing I remember about this dataset is that it was BIG--the full dataset was roughly 10TB and reading in the dataset alone could take hours when I was unfamiliar. 

I ran these analyses on a remote SLURM cluster, so alongside the scripts and notebooks, I also included shell scripts and error/output files. 

Here's a brief descrption of what the code in each folder was intended for.

## 2_10_Percent_Sample_Construction 

To work with this dataset in a more computationally feasible way, most of my analyses were done based on a 10% sample of the 10% sample. This resulted a 1% sample of the total population (so about 3-4 million people). 

This was the code I used to construct the sample. I was learning how to parallelize my code (using packages like multiprocessing and dask), but in this case, I chose to run 180+ jobs in parallel on the cluster, one for each month of data (see `2_Panel_Construction.sh`). 

## 4_Summary_Statistics

Intial data exploration after constructing the panel. Most of my scribbling is in `notebooks`, but the final scripts I ran are in the `code` folder. 

## 5_Mortgages 

Another more detailed set of analyses I ran to look specifically into mortgages. 
