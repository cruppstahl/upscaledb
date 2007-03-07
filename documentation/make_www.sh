\rm -rf html.tar
cd ..
doxygen documentation/Doxyfile.www
cd documentation
./href.pl
rm html_www/*.css
cp doxygen.css html_www/
tar -cvf html.tar html_www/*
