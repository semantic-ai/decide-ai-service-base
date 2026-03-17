# DECIDe AI Service Base package

Base package for implementing AI services in the template for the DECIDe project, containing shared code
for all service implementations.

Currently contains
- Implementations of the AI Annotation types defined in the project (= what all our services produce)
- Prefixes and other SparQL config
- Task base class, for implementing a Task in de Pipeline/Job framework

## Disclaimer
This package expects to be installed in the Python template (as it depends on code therein) and won't work when installed outside of this environment

## Installation
Go to the release page to download the wheel, and install with pip