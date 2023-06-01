from setuptools import find_packages, setup
from pip._internal.req import parse_requirements

dbt_install_reqs = parse_requirements('requirements.txt', session='hack')
dbt_reqs = [ir.requirement for ir in dbt_install_reqs]


migrations_install_reqs = parse_requirements('requirements-migrations.txt', session='hack')
migrations_reqs = [ir.requirement for ir in migrations_install_reqs]

requirements = dbt_reqs + migrations_reqs


setup(
    name='api_dbt',
    packages=find_packages(include=["dbt_airflow"]),
    python_requires=">=3.8",
    version="3.3.0",
    install_requires=requirements,
    description="This library is responsible for building api tables",
    author="Me",
    license="MIT",
    test_suite="tests"
)
