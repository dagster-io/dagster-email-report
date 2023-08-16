from setuptools import find_packages, setup

setup(
    name="user_report",
    packages=find_packages(exclude=["user_report_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb~=0.20",
        "dagster-duckdb-pandas~=0.20",
        "seaborn~=0.12",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
