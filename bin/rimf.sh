echo "replacing $1 -> $2"
for f in include/ham/*.h src/*/*.cc src/*/*.h unittests/*.cpp unittests/*.h samples/*.c* tools/*.c* tools/ham_bench/*.h* tools/ham_bench/*.c*
do
    sed -i "s/$1/$2/g" $f
done
