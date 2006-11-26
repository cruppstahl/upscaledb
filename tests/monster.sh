
d=1 

./run-tests.sh $d --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --overwrite=1 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --inmemorydb=1 $1 $2 $3 $4 $5
./run-tests.sh $d  --overwrite=1 --inmemorydb=1 $1 $2 $3 $4 $5

./run-tests.sh $d --mmap=0 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --mmap=0 --overwrite=1 --reopen=1 $1 $2 $3 $4 $5

./run-tests.sh $d --keysize=8 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --keysize=8 --overwrite=1 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --keysize=8 --inmemorydb=1 $1 $2 $3 $4 $5
./run-tests.sh $d --keysize=8 --overwrite=1 --inmemorydb=1 $1 $2 $3 $4 $5

./run-tests.sh $d --keysize=12 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --keysize=12 --overwrite=1 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --keysize=12 --inmemorydb=1 $1 $2 $3 $4 $5
./run-tests.sh $d --keysize=12 --overwrite=1 --inmemorydb=1 $1 $2 $3 $4 $5

./run-tests.sh $d --keysize=33 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --keysize=33 --overwrite=1 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --keysize=33 --inmemorydb=1 $1 $2 $3 $4 $5
./run-tests.sh $d --keysize=33 --overwrite=1 --inmemorydb=1 $1 $2 $3 $4 $5

./run-tests.sh $d --pagesize=1024 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --pagesize=1024 --overwrite=1 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --pagesize=1024 --inmemorydb=1 $1 $2 $3 $4 $5
./run-tests.sh $d --pagesize=1024 --overwrite=1 --inmemorydb=1 $1 $2 $3 $4 $5

./run-tests.sh $d --pagesize=3072 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --pagesize=3072 --overwrite=1 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --pagesize=3072 --inmemorydb=1 $1 $2 $3 $4 $5
./run-tests.sh $d --pagesize=3072 --overwrite=1 --inmemorydb=1 $1 $2 $3 $4 $5

./run-tests.sh $d --pagesize=8192 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --pagesize=8192 --overwrite=1 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --pagesize=8192 --inmemorydb=1 $1 $2 $3 $4 $5
./run-tests.sh $d --pagesize=8192 --overwrite=1 --inmemorydb=1 $1 $2 $3 $4 $5

./run-tests.sh $d --cachesize=0 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachesize=0 --overwrite=1 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachesize=0 --mmap=0 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachesize=0 --overwrite=1 --mmap=0 --reopen=1 $1 $2 $3 $4 $5

./run-tests.sh $d --cachesize=50 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachesize=50 --overwrite=1 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachesize=50 --mmap=0 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachesize=50 --overwrite=1 --mmap=0 --reopen=1 $1 $2 $3 $4 $5

./run-tests.sh $d --cachesize=1024 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachesize=1024 --overwrite=1 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachesize=1024 --mmap=0 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachesize=1024 --overwrite=1 --mmap=0 --reopen=1 $1 $2 $3 $4 $5

./run-tests.sh $d --cachepolicy=strict --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachepolicy=strict --overwrite=1 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachepolicy=strict --mmap=0 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachepolicy=strict --overwrite=1 --mmap=0 --reopen=1 $1 $2 $3 $4 $5

./run-tests.sh $d --cachepolicy=strict --cachesize=1024 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachepolicy=strict --cachesize=1024 --overwrite=1 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachepolicy=strict --cachesize=1024 --mmap=0 --reopen=1 $1 $2 $3 $4 $5
./run-tests.sh $d --cachepolicy=strict --cachesize=1024 --overwrite=1 --mmap=0 --reopen=1 $1 $2 $3 $4 $5
