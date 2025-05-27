from setuptools import setup, find_packages

setup(
    name="papaya",
    version="0.1.0",
    description="Papaya pyspark",
    author="Your Name",
    author_email="you@example.com",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "papaya": ["plugin_jars/*.jar"],
    },
    python_requires=">=3.7",
    install_requires=[
        "pyspark==3.5.5",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    long_description="A Python wrapper that auto-loads a Spark plugin JAR for Apache Spark jobs.",
    long_description_content_type="text/plain",
)
