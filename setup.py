from setuptools import setup, find_packages

setup(
    name="spark-debugger",
    version="0.1.0",
    description="Spark debugging and monitoring framework",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.13",
    install_requires=[
        "discord.py>=2.3.2",
        "google-genai>=1.14.0",
        "pygithub>=2.6.1",
        "python-dotenv>=1.0.0",
    ],
)
