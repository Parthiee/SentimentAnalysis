#!/bin/bash


PROJECT_HOME="/home/parthiee/Documents/SentimentAnalysis/"

INPUT="$PROJECT_HOME/data.csv"
JAR="$PROJECT_HOME/SentimentAnalysis.jar"
OUTPUT="$PROJECT_HOME/parsed_data.csv"

hadoop fs -rm -r /input
hadoop fs -rm -r /output
hadoop fs -mkdir /input
hadoop fs -put $INPUT /input/input.csv
hadoop jar $JAR SentimentAnalysis /input/input.csv /output
hadoop fs -getmerge /output $OUTPUT






