from setuptools import setup
from os import path
DIR = path.dirname(path.abspath(__file__))
DESCRIPTION = "Monitor and manage fieldwork data collection to control the quality of your research."
AUTHORS = 'Bruce Mkhaliphi (Data Scientist @ ikapadata), Jan Schenk (Dir @ ikapadata)'
URL = 'https://github.com/ikapadata/pykapa'
EMAIL = 'bruce@ikapadata.com'

# install package dependencies
with open(path.join(DIR, 'requirements.txt')) as f:
    INSTALL_PACKAGES = f.read().splitlines()

# open readme.md
with open(path.join(DIR, 'README.md')) as f:
    README = f.read()
# get __version__ from _version.py
'''ver_file = path.join('pykapa', '_version.py')
with open(ver_file) as f:
    exec(f.read())'''

VERSION = '0.0.1'

setup(
name='pykapa',
packages=['pykapa'],
description=DESCRIPTION,
long_description=README,
long_description_content_type='text/markdown',
install_requires=INSTALL_PACKAGES,
version=VERSION,
url=URL,
author=AUTHORS,
author_email=EMAIL,


# STILL HAVE TO EDIT THE LINES BELOW
keywords=['data quality', 'open source research tools', 'research management'],
tests_require=[''],
package_data={
# include json and pkl files
'': ['*.json', 'models/*.pkl', 'models/*.json'],
    },
include_package_data=True,
python_requires='>=3'
)