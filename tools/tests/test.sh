
sh ups_info/compare.sh
if [ $? -ne 0 ]; then
    exit 1
fi

sh ups_dump/compare.sh
if [ $? -ne 0 ]; then
    exit 1
fi

sh export_import.sh
if [ $? -ne 0 ]; then
    exit 1
fi
