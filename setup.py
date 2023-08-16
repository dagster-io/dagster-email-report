from setuptools import find_packages, setup

setup(
    name="user_report",
    packages=find_packages(exclude=["user_report_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
