#!/bin/bash
echo 'SH>CONVERTING'
echo 'SH>----------'
source=$1
format=$2
echo 'SH>SOURCE:'$source
echo 'SH>FORMAT:'$format
soffice --headless --convert-to $format ../$source --outdir ../ 