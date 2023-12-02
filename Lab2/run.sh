#!/bin/bash

echo "=========== Preprocessing ==========="
echo "Start processing document.dat..."
echo "Generating multiple .txt files..."
python preprocess.py

echo "=========== Calculating TF-IDF ==========="  
python tf_idf.py data/* > output/raw_tfidf.txt

echo "=========== Postprocessing ==========="
echo "Converting raw tfidf file into Chinese-encoding file"
python postprocess.py

echo "=========== Searching ==========="
python search.py