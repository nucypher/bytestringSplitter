import os
from distutils.core import setup

BASE_DIR = os.path.dirname(__file__)

ABOUT = dict()
with open(os.path.join(BASE_DIR, "bytestring_splitter", "__about__.py")) as f:
    exec(f.read(), ABOUT)


with open(os.path.join(BASE_DIR, "Readme.md")) as f:
    long_description = f.read()


setup(name=ABOUT['__title__'],
      version=ABOUT['__version__'],
      author=ABOUT['__author__'],
      description=ABOUT['__summary__'],
      long_description=long_description,

      extras_require={"testing": ["bumpversion", "pytest", "pytest-cov"]},
      install_requires=["msgpack-python"],


      classifiers=[
          "Development Status :: 2 - Pre-Alpha",
          "Intended Audience:: Developers",
          "Natural Language :: English",
          "Programming Language :: Python :: Implementation",
          "Programming Language :: Python :: 3 :: Only",
          "Programming Language :: Python :: 3.5",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "Topic :: Software Development :: Libraries :: Python Modules"
        ]
      )
