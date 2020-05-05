from setuptools import find_packages
from setuptools import setup


setup(
    name='tfdv',
    description='TFDV Runtime.',
    version='0.1',
    packages=find_packages(),
    install_requires=[
      'tfx==0.21.4'
    ]
)