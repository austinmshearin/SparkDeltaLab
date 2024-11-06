import setuptools

with open('README.md', 'r', encoding='utf-8') as read_me_file:
    long_description = read_me_file.read()

setuptools.setup(
    name='package',
    version='0.0.1',
    author='Author',
    author_email='author@gmail.com',
    description='Python Project Boiler Plate',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/author/project.git',
    license='MIT',
    packages=[
        'package',
        ],
    install_requires=[
        ],
)
