

from distutils.core import setup, Extension

module1=Extension('hamsterdb', 
      libraries=['hamsterdb'],
      include_dirs=['../include'],
      library_dirs=['../src/.libs'],
      sources=['src/python.cc'])

setup(name='hamsterdb-python', 
      version='2.1.8',
      author='Christoph Rupp',
      author_email='chris@crupp.de',
      url='http://hamsterdb.com',
      description='This is the hamsterdb wrapper for Python',
      license='Apache Public License 2',
      ext_modules=[module1])
