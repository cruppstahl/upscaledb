echo "replacing $1 -> $2"
for f in include/ups/*.h* src/*/*.cc src/*/*.h unittests/*.c* unittests/*.h samples/*.c* tools/*.c* tools/ham_bench/*.h* tools/ham_bench/*.c* java/src/*.c*
do
    sed -i "s/$1/$2/g" $f
done
