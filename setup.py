from setuptools import setup

setup(
    name='discogs_assistant',
    packages=['discogs_assistant'],
    description='Shared Codebase for Discogs Assistant App',
    version='1.0.0',
    url='https://github.com/otosky/discogs_assistant',
    author='Oliver Tosky',
    author_email='olivertosky@gmail.com',
    python_requires='>=3.6',
    install_requires=['pandas>=0.24.2', 'numpy>=1.16.2', 'redis>=3.3.8',
                      'discogs_client>=2.2.2', 'scipy>=1.2.1', 'psycopg2>=2.7.6.1',
                      'google-cloud-pubsub>=1.0.2', 'google-cloud-logging>=1.14.0',
                      'google-cloud-storage>=1.23.0', 'pymongo>=3.8.0',
                      'lxml>=4.3.2', 'tenacity>=6.0.0'
    ]
    )