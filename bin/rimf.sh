echo "replacing $1 -> $2"
for f in src/*.cc src/*.h unittests/*.cpp
do
    sed -i "s/$1/$2/g" $f
done
