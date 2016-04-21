from setuptools import setup


setup(
    name='tasker',
    version='0.0.1',
    author='gal@intsights.com',
    author_email='gal@intsights.com',
    description=('A fast, simple, task distribution library'),
    zip_safe=True,
    packages=[
        'tasker',
        'tasker.connectors',
        'tasker.monitor',
    ],
)
