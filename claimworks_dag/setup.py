from setuptools import find_packages, setup

setup(
    name="claimworks_dag",
    packages=find_packages(exclude=["claimworks_dag_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
