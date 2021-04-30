from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='jasyncq',
    version='1.1.1',
    description='High reliability asynchronous queue using mysql(lock)',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='pjongy',
    author_email='hi.pjongy@gmail.com',
    url='https://github.com/pjongy/jasyncq',
    install_requires=[
        'pydantic==1.7.3',
        'aiomysql>=0.0.20',
        'PyPika>=0.37.6',
    ],
    packages=find_packages(),
    keywords=['message queue', 'queue', 'distributed', 'microservice'],
    python_requires='>=3.7',
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8'
    ]
)
