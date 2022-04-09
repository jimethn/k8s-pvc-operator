from setuptools import setup, find_packages

import operator_pvc_manager

# Load app and test requirements
requirements = []
test_requirements = []
with open('requirements.txt', 'r') as rh:
    for requirement in rh.read().splitlines():
        requirement = requirement.strip()
        if not requirement or requirement.startswith('#'):
            continue
        requirements.append(requirement)

test_requirements = [
    'pytest',
    'pytest-cov',
    'pytest-xdist',
    ]

setup(
    name='operator_pvc_manager',
    packages=list(find_packages()),
    version=1.0,
    description='Operator for managing Kubernetes PVCs',
    author='Jonathan Lynch',
    author_email='jimethn@gmail.com',
    url='https://github.com/jimethn/k8s-pvc-operator',
    setup_requires=['pytest-runner'],
    install_requires=requirements,
    tests_require=test_requirements,
    test_suite='tests',
)
