 
#sh short.sh  $1 $2 $3 $4 $5
#sh short.sh --inmemorydb=1 $1 $2 $3 $4 $5
#sh short.sh --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --mmap=0 $1 $2 $3 $4 $5
#sh short.sh --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

#sh short.sh --pagesize=1024 $1 $2 $3 $4 $5
#sh short.sh --pagesize=1024 --inmemorydb=1 $1 $2 $3 $4 $5
#sh short.sh --pagesize=1024 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --pagesize=1024 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5

#sh short.sh --pagesize=3072 $1 $2 $3 $4 $5
#sh short.sh --pagesize=3072 --inmemorydb=1 $1 $2 $3 $4 $5
#sh short.sh --pagesize=3072 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --pagesize=3072 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5

#sh short.sh --pagesize=8192 $1 $2 $3 $4 $5
#sh short.sh --pagesize=8192 --inmemorydb=1 $1 $2 $3 $4 $5
#sh short.sh --pagesize=8192 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --pagesize=8192 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5

#sh short.sh --keysize=8 $1 $2 $3 $4 $5
#sh short.sh --keysize=8 --inmemorydb=1 $1 $2 $3 $4 $5
#sh short.sh --keysize=8 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --keysize=8 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --keysize=8 --mmap=0 $1 $2 $3 $4 $5
#sh short.sh --keysize=8 --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

#sh short.sh --keysize=16 $1 $2 $3 $4 $5
#sh short.sh --keysize=16 --inmemorydb=1 $1 $2 $3 $4 $5
#sh short.sh --keysize=16 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --keysize=16 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --keysize=16 --mmap=0 $1 $2 $3 $4 $5
#sh short.sh --keysize=16 --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

#sh short.sh --keysize=128 $1 $2 $3 $4 $5
#sh short.sh --keysize=128 --inmemorydb=1 $1 $2 $3 $4 $5
#sh short.sh --keysize=128 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --keysize=128 --inmemorydb=1 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --keysize=128 --mmap=0 $1 $2 $3 $4 $5
#sh short.sh --keysize=128 --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

#sh short.sh --cachesize=0 $1 $2 $3 $4 $5
#sh short.sh --cachesize=0 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --cachesize=0 --mmap=0 $1 $2 $3 $4 $5
#sh short.sh --cachesize=0 --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

#sh short.sh --cachesize=50 $1 $2 $3 $4 $5
#sh short.sh --cachesize=50 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --cachesize=50 --mmap=0 $1 $2 $3 $4 $5
#sh short.sh --cachesize=50 --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

#sh short.sh --cachesize=1048576 $1 $2 $3 $4 $5
#sh short.sh --cachesize=1048576 --overwrite=1 $1 $2 $3 $4 $5
#sh short.sh --cachesize=1048576 --mmap=0 $1 $2 $3 $4 $5
#sh short.sh --cachesize=1048576 --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

sh short.sh --cachepolicy=strict $1 $2 $3 $4 $5
sh short.sh --cachepolicy=strict --overwrite=1 $1 $2 $3 $4 $5
sh short.sh --cachepolicy=strict --mmap=0 $1 $2 $3 $4 $5
sh short.sh --cachepolicy=strict --mmap=0 --overwrite=1 $1 $2 $3 $4 $5

sh short.sh --cachesize=1024 --cachepolicy=strict $1 $2 $3 $4 $5
sh short.sh --cachesize=1024 --cachepolicy=strict --overwrite=1 $1 $2 $3 $4 $5
sh short.sh --cachesize=1024 --cachepolicy=strict --mmap=0 $1 $2 $3 $4 $5
sh short.sh --cachesize=1024 --cachepolicy=strict --mmap=0 --overwrite=1 $1 $2 $3 $4 $5
