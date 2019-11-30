#!/bin/bash
echo 'PDFSH>Generating PDF'
echo 'PDFSH>---------------'
source=$1
target=$2
echo 'PDFSH>SOURCE:'$source
echo 'PDFSH>TARGET:'$target
#soffice --headless --convert-to pdf $source --outdir $target
libreoffice6.3 --headless --convert-to pdf $source --outdir $target

