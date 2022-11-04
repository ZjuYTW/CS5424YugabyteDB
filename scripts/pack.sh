mkdir -p package/data/table_files
mkdir -p package/bin
cp data/table_files/* package/data/table_files
cp scripts/* package
cp build/src/*_perf package/bin
zip -r package.zip package/