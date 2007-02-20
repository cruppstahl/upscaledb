\rm -rf html.tar
cd ..
doxygen documentation/Doxyfile.www
cd documentation
./href.pl
rm html_www/*.css
tar -cvf html.tar html_www/*
