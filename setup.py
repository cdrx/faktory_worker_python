from setuptools import setup

setup(name='faktory_worker',
      version='0.1',
      description='Python worker for the Faktory project',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: BSD License',
          'Programming Language :: Python :: 3',
          'Topic :: System :: Distributed Computing',
      ],
      keywords='faktory worker',
      url='http://github.com/cdrx/faktory_python_worker',
      author='Chris R',
      license='BSD',
      packages=['faktory_worker'],
      zip_safe=False)
