'''
Template thanks to Jeffrey Ness' tutorial video:
"Better Python Testing: How to Mock AWS"
https://www.youtube.com/watch?v=11Fr0wqcxRc
'''

from setuptools import setup, find_packages

setup(
    name='convert_json_to_dataframe',
    version='0.0.1',
    
    description='sample_description',
    license='',
    
    author='Drew Engberson',
    author_email='drew.engberson@slalom.com',
    
    packages=find_packages(
        exclude=['tests']
    ),
    
    test_suite='tests',
    
    install_requires=[
        'boto3',
        'awswrangler'
    ],
    
    tests_require=[
        'moto'
    ],
    
    entry_points={
        'console_scripts': [
            's3 = convert_json_to_dataframe.s3:main',
            'route53 = convert_json_to_dataframe.route53:main'
        ]
    }
)