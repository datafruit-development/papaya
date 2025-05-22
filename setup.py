from setuptools import setup, find_packages

setup(
    name="papaya-debugger",
    version="0.1.1",
    description="Spark debugging and monitoring framework",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.10",
    install_requires=[
        "discord.py>=2.3.2",
        "google-genai>=1.14.0",
        "pygithub>=2.6.1",
        "python-dotenv>=1.0.0",
        "requests>=2.32.0",
    ],
    entry_points={
        'console_scripts': [
            'papaya=papaya.cli:main',
        ],
    },
)
