import os
import sys

from setuptools import setup, find_packages
from setuptools.command.install import install


#
# Metadata & Deps
#

PACKAGE_NAME = "bytestring_splitter"
BASE_DIR = os.path.dirname(__file__)

ABOUT = dict()
with open(os.path.join(BASE_DIR, PACKAGE_NAME, "__about__.py")) as f:
    exec(f.read(), ABOUT)


with open(os.path.join(BASE_DIR, "README.md")) as f:
    long_description = f.read()

with open(os.path.join(BASE_DIR, "requirements.txt")) as f:
    INSTALL_REQUIRES = f.read().split('\n')

with open(os.path.join(BASE_DIR, "dev-requirements.txt")) as f:
    TESTS_REQUIRE = f.read().split('\n')


#
# Utility
#

class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""
    description = 'verify that the git tag matches our version'

    def run(self):
        tag = os.getenv('CIRCLE_TAG')
        if tag.startswith('v'):
            tag = tag[1:]

        version = ABOUT['__version__']
        if version.startswith('v'):
            version = version[1:]

        if tag != version:
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                os.getenv('CIRCLE_TAG'), ABOUT['__version__']
            )
            sys.exit(info)


#
# Main
#

setup(name=ABOUT['__title__'],
      url=ABOUT['__url__'],
      version=ABOUT['__version__'],
      author=ABOUT['__author__'],
      author_email=ABOUT['__email__'],
      description=ABOUT['__summary__'],
      license=ABOUT['__license__'],
      long_description=long_description,

      setup_requires=['pytest-runner'],  # required for setup.py test
      tests_require=TESTS_REQUIRE,

      extras_require={"testing": TESTS_REQUIRE},
      install_requires=INSTALL_REQUIRES,

      packages=find_packages(exclude=["tests"]),
      cmdclass={'verify': VerifyVersionCommand},

      classifiers=[
          "Development Status :: 2 - Pre-Alpha",
          "Natural Language :: English",
          "Programming Language :: Python :: Implementation",
          "Programming Language :: Python :: 3 :: Only",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "Topic :: Software Development :: Libraries :: Python Modules"
        ]
      )
