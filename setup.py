import os

from setuptools import setup

from nats import __version__

this_dir = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(this_dir, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

setup(
    name='tx-nats-client',
    version=__version__,
    description='NATS client for Python 2',
    long_description='Twisted based Python client for NATS',
    url='https://github.com/saucelabs/python-nats',
    author='Alex Plischke',
    author_email='alex.plischke@saucelabs.com',
    license='Apache 2.0 License',
    packages=['nats', 'nats.io', 'nats.protocol'],
    install_requires=requirements,
    zip_safe=True,
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
    ]
)
