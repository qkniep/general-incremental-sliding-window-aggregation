mkdir build
latexmk -pdf -jobname=./build/report report
cp ./build/report.pdf ./
