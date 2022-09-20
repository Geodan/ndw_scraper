#!/usr/bin/env bash

awk 'BEGIN { FS = "\t"; OFS = "\t" }
     {
         if (NR == FNR)
             values[$1] = $2
         else
             print $1, values[$2], $3, $4
     }' meas.csv speed.csv

