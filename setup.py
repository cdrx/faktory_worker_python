import sys
from setuptools import setup

dev_requires = open("dev-requirements.txt").read().strip().split("\n")
test_requires = open("test-requirements.txt").read().strip().split("\n")

extras = {"dev": dev_requires + test_requires, "test": test_requires}

if sys.version_info < (3, 6):
    extras["dev"].remove("black")

extras["all_extras"] = sum(extras.values(), [])

setup(
    name="faktory",
    version="0.5.0",
    description="Python worker for the Faktory project",
    extras_require=extras,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3",
        "Topic :: System :: Distributed Computing",
    ],
    keywords="faktory worker",
    url="http://github.com/cdrx/faktory_python_worker",
    author="Chris R",
    license="BSD",
    packages=["faktory"],
    zip_safe=False,
)
