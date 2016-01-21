echo "replacing $1 -> $2"
for f in include/ups/*.h* src/*/*.cc src/*/*.h unittests/*.c* unittests/*.h* samples/*.c* tools/*.c* tools/*.h* tools/ups_bench/*.h* tools/ups_bench/*.c* java/src/*.c*
do
    sed -i "s/$1/$2/g" $f
done
