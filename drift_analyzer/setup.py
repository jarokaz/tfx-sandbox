from setuptools import find_packages
from setuptools import setup


setup(
    name='drift_analyzer',
    description='Data drift analyzer.',
    version='0.1',
    packages=find_packages(),
    install_requires=[
      'tensorflow-data-validation'
    ]
)