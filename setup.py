import setuptools

with open('README.md', 'r', encoding='utf-8') as read_me_file:
    long_description = read_me_file.read()

setuptools.setup(
    name='delta_analytics',
    version='0.0.1',
    author='Austin Shearin',
    author_email='austinmshearin@gmail.com',
    description='Delta Table Analytics',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/austinmshearin/SparkDeltaLab',
    license='MIT',
    packages=[
        'delta_analytics',
        ],
    install_requires=[
        'pyspark'
        ],
)
