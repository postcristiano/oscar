#!/bin/bash

directory_path="/path/to/your/directory"
substring_to_remove="xxx"

cd "$directory_path" || exit

for filename in *"$substring_to_remove"*; do
    new_filename="${filename//$substring_to_remove/}"
    mv "$filename" "$new_filename"
    echo "Renamed $filename to $new_filename"
done
