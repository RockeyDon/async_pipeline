from setuptools import setup, find_packages
from os import path

DIR = path.dirname(path.abspath(__file__))
with open(path.join(DIR, 'requirements/release.txt')) as f:
    INSTALL_PACKAGES = f.read().splitlines()
with open(path.join(DIR, 'requirements/test.txt')) as f:
    TEST_PACKAGES = f.read().splitlines()
with open(path.join(DIR, 'README.md')) as f:
    README = f.read()


def find_this_packages():
    packages = find_packages()
    packages.remove('tests')
    return packages


setup(
    name='async_pipeline',
    version='0.1.2',
    author='Rockey Don',
    author_email='bjxdrj@gmail.com',
    description="Async pipeline with functional methods",
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://github.com/RockeyDon/async_pipeline',
    packages=find_this_packages(),
    keywords=['async', 'pipeline', 'functional'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    install_requires=INSTALL_PACKAGES,
    setup_requires=[],
    tests_require=TEST_PACKAGES,
    python_requires='>=3.7, <3.11',
)
