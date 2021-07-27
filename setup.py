from setuptools import setup, find_packages


def readme():
    with open('README.md') as f:
        return f.read()


setup(
    name='flink-rest-client',
    version='1.0.4',
    description='Easy to use Flink REST API client implementation',
    long_description=readme(),
    long_description_content_type='text/markdown',
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
