mkdir -p package
mkdir -p package/data
mkdir -p package/bin
cp scripts/* package
cp build/src/*_perf package/bin
zip -r package.zip package/