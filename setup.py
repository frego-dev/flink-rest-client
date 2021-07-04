from setuptools import setup, find_packages

setup(
    name='flink-rest-client',
    version='0.3.0',
    description='Easy to use Flink REST API client implementation',
    url='https://github.com/frego-dev/flink-rest-client',
    author='Attila Nagy',
    author_email='anagy@frego.dev',
    license='MIT',
    packages=find_packages(),
    install_requires=['requests', 'importlib_resources'],
    entry_points={
          'console_scripts': [],
    }
)
