from setuptools import setup, find_packages

setup(
    name='salesforce-to-datalake-integrator',
    version='0.1.0',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'salesforce_to_datalake=salesforce_to_datalake_integrator.salesforce_to_db:main',
        ],
    },
    author='Bhargav Velisetti, Karthik Reddy Banka, Agampaul Singh',
    author_email='bhargav.v14@gmail.com, karthikreddybv2@gmail.com, apsingh.csis@gmail.com',
    description='A project to integrate Salesforce data with a data lake',
    url='https://github.com/yourusername/salesforce-to-datalake-integrator',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)