echo "replacing $1 -> $2"
for f in src/*.cc src/server/*.cc src/*.h unittests/*.cpp samples/*.c* tools/*.c*
do
    sed -i "s/$1/$2/g" $f
done
