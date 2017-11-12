from setuptools import setup

setup(name='faktory',
      version='0.2',
      description='Python worker for the Faktory project',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: BSD License',
          'Programming Language :: Python :: 3',
          'Topic :: System :: Distributed Computing'
      ],
      keywords='faktory worker',
      url='http://github.com/cdrx/faktory_python_worker',
      author='Chris R',
      license='BSD',
      packages=['faktory'],
      zip_safe=False)
