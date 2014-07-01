
all:
	curl -H 'User-Agent: cruppstahl' -X POST -d @test.txt https://api.github.com/markdown/raw --header "Content-Type:text/plain" > test.html
	curl -H 'User-Agent: cruppstahl' -X POST -d @ham_env_create.txt https://api.github.com/markdown/raw --header "Content-Type:text/plain" > ham_env_create.html
