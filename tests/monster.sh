 
#sh full.sh  $1 $2 $3 $4 $5
#sh full.sh --inmemorydb=1 $1 $2 $3 $4 $5
#sh full.sh --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --mmap=0 $1 $2 $3 $4 $5
sh full.sh --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

sh full.sh --pagesize=1024 $1 $2 $3 $4 $5
sh full.sh --pagesize=1024 --inmemorydb=1 $1 $2 $3 $4 $5
sh full.sh --pagesize=1024 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --pagesize=1024 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5

sh full.sh --pagesize=3072 $1 $2 $3 $4 $5
sh full.sh --pagesize=3072 --inmemorydb=1 $1 $2 $3 $4 $5
sh full.sh --pagesize=3072 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --pagesize=3072 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5

sh full.sh --pagesize=8192 $1 $2 $3 $4 $5
sh full.sh --pagesize=8192 --inmemorydb=1 $1 $2 $3 $4 $5
sh full.sh --pagesize=8192 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --pagesize=8192 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5

sh full.sh --keysize=8 $1 $2 $3 $4 $5
sh full.sh --keysize=8 --inmemorydb=1 $1 $2 $3 $4 $5
sh full.sh --keysize=8 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --keysize=8 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --keysize=8 --mmap=0 $1 $2 $3 $4 $5
sh full.sh --keysize=8 --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

sh full.sh --keysize=16 $1 $2 $3 $4 $5
sh full.sh --keysize=16 --inmemorydb=1 $1 $2 $3 $4 $5
sh full.sh --keysize=16 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --keysize=16 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --keysize=16 --mmap=0 $1 $2 $3 $4 $5
sh full.sh --keysize=16 --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

sh full.sh --keysize=128 $1 $2 $3 $4 $5
sh full.sh --keysize=128 --inmemorydb=1 $1 $2 $3 $4 $5
sh full.sh --keysize=128 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --keysize=128 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --keysize=128 --mmap=0 $1 $2 $3 $4 $5
sh full.sh --keysize=128 --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

sh full.sh --cachesize=0 $1 $2 $3 $4 $5
sh full.sh --cachesize=0 --inmemorydb=1 $1 $2 $3 $4 $5
sh full.sh --cachesize=0 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --cachesize=0 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --cachesize=0 --mmap=0 $1 $2 $3 $4 $5
sh full.sh --cachesize=0 --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

sh full.sh --cachesize=50 $1 $2 $3 $4 $5
sh full.sh --cachesize=50 --inmemorydb=1 $1 $2 $3 $4 $5
sh full.sh --cachesize=50 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --cachesize=50 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --cachesize=50 --mmap=0 $1 $2 $3 $4 $5
sh full.sh --cachesize=50 --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

sh full.sh --cachesize=1048576 $1 $2 $3 $4 $5
sh full.sh --cachesize=1048576 --inmemorydb=1 $1 $2 $3 $4 $5
sh full.sh --cachesize=1048576 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --cachesize=1048576 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --cachesize=1048576 --mmap=0 $1 $2 $3 $4 $5
sh full.sh --cachesize=1048576 --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

sh full.sh --cachepolicy=strict $1 $2 $3 $4 $5
sh full.sh --cachepolicy=strict --inmemorydb=1 $1 $2 $3 $4 $5
sh full.sh --cachepolicy=strict --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --cachepolicy=strict --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --cachepolicy=strict --mmap=0 $1 $2 $3 $4 $5
sh full.sh --cachepolicy=strict --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

sh full.sh --cachesize=1024 --cachepolicy=strict $1 $2 $3 $4 $5
sh full.sh --cachesize=1024 --cachepolicy=strict --inmemorydb=1 $1 $2 $3 $4 $5
sh full.sh --cachesize=1024 --cachepolicy=strict --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --cachesize=1024 --cachepolicy=strict --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
sh full.sh --cachesize=1024 --cachepolicy=strict --mmap=0 $1 $2 $3 $4 $5
sh full.sh --cachesize=1024 --cachepolicy=strict --mmap=0 --overwrite=1 $1 $2 $3 $4 $5
